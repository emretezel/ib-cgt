"""The SA108 report model — the shape `ib-cgt report` renders.

A persisted tax run holds matched chunks and futures realisations in
the engines' own terms. HMRC wants the same facts in the terms of the
SA108 "Capital Gains Tax summary" pages: a handful of box totals per
section of the form, backed by one computation per disposal laid out
like the notes' working sheet (proceeds A, incidental costs of
disposal B, net proceeds C, cost D, incidental costs of acquisition
E, total costs G, gain or loss H). This module defines that shape and
nothing else — the arithmetic that fills it lives in `builder.py`,
the tables that present it in `layout.py`.

Every class is a frozen dataclass with the same discipline as the
domain layer: tuples not lists, invariants enforced in
`__post_init__`, derived figures exposed as properties rather than
stored twice. `Money` amounts are carried unrounded; rounding to
pennies is the renderers' job.

Author: Emre Tezel
"""

from __future__ import annotations

import decimal
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Final, Literal

from ib_cgt.domain import (
    AnyInstrument,
    AssetClass,
    IssueSeverity,
    MatchRule,
    Money,
    OptionCloseKind,
    RunIssue,
    TaxYear,
)

# ---------------------------------------------------------------------------
# SA108 sections and their box numbers
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class BoxNumbers:
    """The five SA108 box numbers one section of the form uses.

    Attributes:
        disposals: "Number of disposals".
        proceeds: "Disposal proceeds".
        allowable_costs: "Allowable costs (including purchase price)".
        gains: "Gains in the year, before losses".
        losses: "Losses in the year".
    """

    disposals: int
    proceeds: int
    allowable_costs: int
    gains: int
    losses: int


class Sa108SectionKind(StrEnum):
    """The two SA108 sections an IB history can populate.

    Listed shares and securities take the share-matched classes (stocks
    and non-exempt bonds — IB's bonds are exchange-listed). Futures
    close-outs (TCGA 1992 s.143), options (s.144: an option is an asset
    in its own right, not a share or security) and foreign-currency
    pools fall under "Other property, assets and gains", the section
    the notes reserve for "assets not covered elsewhere".
    """

    LISTED_SHARES = "listed_shares"
    OTHER_ASSETS = "other_assets"

    @property
    def heading(self) -> str:
        """The section heading as printed on the form."""
        return _TITLES[self]

    @property
    def boxes(self) -> BoxNumbers:
        """The section's box numbers on the 2025-26 form."""
        return _BOXES[self]

    @classmethod
    def for_asset_class(cls, asset_class: AssetClass) -> Sa108SectionKind:
        """Which section a class of disposal is reported in."""
        return _SECTION_OF[asset_class]


_TITLES: Final[dict[Sa108SectionKind, str]] = {
    Sa108SectionKind.LISTED_SHARES: "Listed shares and securities",
    Sa108SectionKind.OTHER_ASSETS: "Other property, assets and gains",
}

_BOXES: Final[dict[Sa108SectionKind, BoxNumbers]] = {
    Sa108SectionKind.LISTED_SHARES: BoxNumbers(
        disposals=23, proceeds=24, allowable_costs=25, gains=26, losses=27
    ),
    Sa108SectionKind.OTHER_ASSETS: BoxNumbers(
        disposals=14, proceeds=15, allowable_costs=16, gains=17, losses=19
    ),
}

_SECTION_OF: Final[dict[AssetClass, Sa108SectionKind]] = {
    AssetClass.STOCK: Sa108SectionKind.LISTED_SHARES,
    AssetClass.BOND: Sa108SectionKind.LISTED_SHARES,
    AssetClass.FUTURE: Sa108SectionKind.OTHER_ASSETS,
    AssetClass.FX: Sa108SectionKind.OTHER_ASSETS,
    AssetClass.OPTION: Sa108SectionKind.OTHER_ASSETS,
}


# ---------------------------------------------------------------------------
# Exact arithmetic
# ---------------------------------------------------------------------------

# The engines convert at spot and never round, so a persisted amount can
# carry the full 28 significant digits of Decimal's default context.
# Adding hundreds of such amounts at that precision rounds every partial
# sum, and the working-sheet identity proceeds - costs == gains - losses
# then misses by a few 1e-24. Every sum and difference the report takes
# therefore runs at a precision wide enough for the arithmetic to be
# exact: 28-digit operands with bounded exponents need well under 60.
_EXACT_PRECISION: Final = 60


@contextmanager
def exact_arithmetic() -> Iterator[None]:
    """A Decimal context in which the report's sums and differences are exact."""
    with decimal.localcontext(prec=_EXACT_PRECISION):
        yield


# ---------------------------------------------------------------------------
# Box figures
# ---------------------------------------------------------------------------


def _require_gbp(name: str, value: Money) -> None:
    """Every reported amount is sterling; anything else is a programming error."""
    if not value.is_gbp():
        raise ValueError(f"{name} must be GBP, got {value.currency}")


@dataclass(frozen=True, slots=True, kw_only=True)
class Sa108Figures:
    """The five figures one SA108 section asks for, in GBP.

    `gains_gbp` and `losses_gbp` are non-negative magnitudes summed over
    the computations that ended positive and negative respectively;
    the form wants them separately ("gains in the year, before
    losses" / "losses in the year"). Because allowable costs include
    the incidental costs of disposal, `proceeds - allowable costs`
    equals `gains - losses` exactly, and the constructor insists on it.

    Attributes:
        disposal_count: HMRC disposals (one per instrument per day).
        proceeds_gbp: Gross disposal proceeds, before any fee.
        allowable_costs_gbp: Purchase cost plus every incidental cost,
            on both the acquisition and the disposal side.
        gains_gbp: Sum of the positive outcomes.
        losses_gbp: Sum of the negative outcomes, as a magnitude.
    """

    disposal_count: int
    proceeds_gbp: Money
    allowable_costs_gbp: Money
    gains_gbp: Money
    losses_gbp: Money

    def __post_init__(self) -> None:
        """Sterling, non-negative where the form expects it, and internally consistent."""
        for name in ("proceeds_gbp", "allowable_costs_gbp", "gains_gbp", "losses_gbp"):
            _require_gbp(f"Sa108Figures.{name}", getattr(self, name))
        if self.disposal_count < 0:
            raise ValueError(f"Sa108Figures.disposal_count must be >= 0, got {self.disposal_count}")
        if self.gains_gbp.amount < 0:
            raise ValueError(f"Sa108Figures.gains_gbp must be >= 0, got {self.gains_gbp}")
        if self.losses_gbp.amount < 0:
            raise ValueError(f"Sa108Figures.losses_gbp must be >= 0, got {self.losses_gbp}")
        with exact_arithmetic():
            reconciles = self.proceeds_gbp - self.allowable_costs_gbp == self.net_gbp
        if not reconciles:
            raise ValueError(
                "Sa108Figures: proceeds - allowable costs must equal gains - losses "
                f"({self.proceeds_gbp} - {self.allowable_costs_gbp} vs {self.net_gbp})"
            )

    @classmethod
    def zero(cls) -> Sa108Figures:
        """An empty section: every box zero."""
        zero = Money.zero("GBP")
        return cls(
            disposal_count=0,
            proceeds_gbp=zero,
            allowable_costs_gbp=zero,
            gains_gbp=zero,
            losses_gbp=zero,
        )

    @property
    def net_gbp(self) -> Money:
        """Gains less losses — the section's net outcome (not a box on the form)."""
        with exact_arithmetic():
            return self.gains_gbp - self.losses_gbp

    def __add__(self, other: Sa108Figures) -> Sa108Figures:
        """Box-by-box sum, used to roll classes into sections and sections into the year."""
        with exact_arithmetic():
            return Sa108Figures(
                disposal_count=self.disposal_count + other.disposal_count,
                proceeds_gbp=self.proceeds_gbp + other.proceeds_gbp,
                allowable_costs_gbp=self.allowable_costs_gbp + other.allowable_costs_gbp,
                gains_gbp=self.gains_gbp + other.gains_gbp,
                losses_gbp=self.losses_gbp + other.losses_gbp,
            )


@dataclass(frozen=True, slots=True, kw_only=True)
class AssetClassFigures:
    """One asset class's contribution to a section's figures."""

    asset_class: AssetClass
    figures: Sa108Figures


@dataclass(frozen=True, slots=True, kw_only=True)
class Sa108Section:
    """One section of the form: its box figures and where they came from.

    Attributes:
        kind: Which section.
        figures: The box figures for the section as a whole.
        by_asset_class: The same figures split by asset class, in
            `AssetClass` order, one entry per class that contributed
            at least one disposal. The section's figures must equal
            their sum.
    """

    kind: Sa108SectionKind
    figures: Sa108Figures
    by_asset_class: tuple[AssetClassFigures, ...]

    def __post_init__(self) -> None:
        """Every class belongs to this section, appears once, and the parts sum to the whole."""
        seen: set[AssetClass] = set()
        total = Sa108Figures.zero()
        for contribution in self.by_asset_class:
            if Sa108SectionKind.for_asset_class(contribution.asset_class) is not self.kind:
                raise ValueError(
                    f"Sa108Section {self.kind.value}: {contribution.asset_class.value} "
                    "is reported in a different section"
                )
            if contribution.asset_class in seen:
                raise ValueError(
                    f"Sa108Section {self.kind.value}: duplicate asset class "
                    f"{contribution.asset_class.value}"
                )
            seen.add(contribution.asset_class)
            total = total + contribution.figures
        if total != self.figures:
            raise ValueError(
                f"Sa108Section {self.kind.value}: figures do not equal the sum of the "
                "per-asset-class figures"
            )


# ---------------------------------------------------------------------------
# Computations — one per HMRC disposal
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class EventRef:
    """A citeable reference to the event behind a line: a trade, a dividend, a cash row.

    The engines key everything by integer id — a real `trades.trade_id`
    or, for the FX pools' non-trade cashflows, a synthetic id. The
    report resolves each id once into the label the audit commands
    print (`#N`, `Div #N`, `WHT #N`, `Cpn #N`, `Cash #N`,
    `P&L #a→#b`), the date the event happened, the account it sat in
    and a one-line description, so a reader can find the row in the
    original statement without the tool.

    Attributes:
        event_id: The id the engines used — kept for traceability.
        label: The citeable label (`docs/audit.md` conventions).
        on: The event date, or `None` when the row could not be found.
        account_id: The IB account, or `None` when unresolved.
        description: What the event was, in words.
    """

    event_id: int
    label: str
    on: date | None
    account_id: str | None
    description: str


@dataclass(frozen=True, slots=True, kw_only=True)
class DirectBasis:
    """The line's cost came from one identified acquisition (same-day, 30-day, later)."""

    rule: MatchRule
    acquisition: EventRef

    def __post_init__(self) -> None:
        """The pool rule has its own basis shape."""
        if self.rule is MatchRule.SECTION_104:
            raise ValueError("DirectBasis cannot carry the section_104 rule; use PoolBasis")


@dataclass(frozen=True, slots=True, kw_only=True)
class PoolBasis:
    """The line's cost came from the S.104 holding at its average cost.

    Attributes:
        quantity_before: Units in the pool immediately before the draw.
        total_cost_gbp_before: The pool's total allowable cost before the draw.
        average_cost_gbp: Cost per unit the draw was priced at.
    """

    quantity_before: Decimal
    total_cost_gbp_before: Money
    average_cost_gbp: Money

    @property
    def rule(self) -> MatchRule:
        """A pool draw is always the section 104 rule."""
        return MatchRule.SECTION_104


@dataclass(frozen=True, slots=True, kw_only=True)
class CloseOutBasis:
    """A futures contract closed out against the trade that opened it (TCGA 1992 s.143).

    The native figures reconcile against IB's per-trade "Realized P&L"
    and commissions; the two rates are the "1 GBP = r native" spots
    the engine applied on each cashflow's own date.

    Attributes:
        side: Whether the closed position was LONG or SHORT.
        open: The opening trade.
        gross_pnl_native: Signed profit or loss in the contract currency.
        open_fee_native: Commission on the opening leg, contract currency.
        close_fee_native: Commission on the closing leg, contract currency.
        open_fx_rate: Spot applied to the opening commission.
        close_fx_rate: Spot applied to the P&L and the closing commission.
    """

    side: Literal["LONG", "SHORT"]
    open: EventRef
    gross_pnl_native: Money
    open_fee_native: Money
    close_fee_native: Money
    open_fx_rate: Decimal
    close_fx_rate: Decimal


@dataclass(frozen=True, slots=True, kw_only=True)
class GrantCloseRef:
    """One later event on a written option's grant, as the report cites it.

    Attributes:
        close: The closing trade — a purchase, a lapse, an assignment
            or a cash settlement.
        kind: Which of those it was.
        quantity: Contracts of the grant it closed.
        premium_native: Premium paid on that portion (zero for a lapse
            or an assignment), series currency.
        fee_native: The closing row's commission share.
        fx_rate: Spot applied on the close date.
        cost_gbp: What it added to the grant's incidental costs.
    """

    close: EventRef
    kind: OptionCloseKind
    quantity: Decimal
    premium_native: Money
    fee_native: Money
    fx_rate: Decimal
    cost_gbp: Money


@dataclass(frozen=True, slots=True, kw_only=True)
class GrantBasis:
    """A written option: the grant is the disposal (TCGA 1992 s.144(1)).

    There is no acquisition — the writer never owned the option — so
    the line has no D or E. A is the premium still charged on the
    grant, B the grant commission plus every closing purchase or cash
    settlement (s.148(3), s.144A). Contracts later assigned have left
    the grant for the share trade and are not in A.

    Attributes:
        premium_native: The whole grant's gross premium, series currency.
        grant_fee_native: The grant row's commission.
        grant_fx_rate: Spot applied on the grant date.
        granted_quantity: Contracts written.
        chargeable_quantity: Contracts still charged on this grant.
        closes: Every later event on the grant, in drain order.
    """

    premium_native: Money
    grant_fee_native: Money
    grant_fx_rate: Decimal
    granted_quantity: Decimal
    chargeable_quantity: Decimal
    closes: tuple[GrantCloseRef, ...]


LineBasis = DirectBasis | PoolBasis | CloseOutBasis | GrantBasis
"""How one computation line's allowable cost was identified."""


@dataclass(frozen=True, slots=True, kw_only=True)
class ComputationLine:
    """One identification within a disposal — one row of the HMRC working sheet.

    The four stored amounts are the working sheet's inputs; the three
    derived ones (`net_proceeds_gbp`, `allowable_costs_gbp`,
    `gain_gbp`) are C = A - B, G = D + E and H = C - G.

    Attributes:
        disposal: The disposal event this line belongs to.
        matched_quantity: Units (shares, bond units, currency units,
            contracts) identified on this line.
        basis: Where the cost came from.
        gross_proceeds_gbp: A — proceeds before incidental costs.
        disposal_costs_gbp: B — incidental costs of disposal (sale fees).
        cost_gbp: D — the purchase price (or, for a losing futures
            close-out, the amount paid on close-out).
        acquisition_costs_gbp: E — incidental costs of acquisition.
    """

    disposal: EventRef
    matched_quantity: Decimal
    basis: LineBasis
    gross_proceeds_gbp: Money
    disposal_costs_gbp: Money
    cost_gbp: Money
    acquisition_costs_gbp: Money

    def __post_init__(self) -> None:
        """Sterling throughout; the costs the form adds up cannot be negative."""
        for name in (
            "gross_proceeds_gbp",
            "disposal_costs_gbp",
            "cost_gbp",
            "acquisition_costs_gbp",
        ):
            _require_gbp(f"ComputationLine.{name}", getattr(self, name))
        if self.matched_quantity <= 0:
            raise ValueError(
                f"ComputationLine.matched_quantity must be > 0, got {self.matched_quantity}"
            )
        for name in ("disposal_costs_gbp", "cost_gbp", "acquisition_costs_gbp"):
            value: Money = getattr(self, name)
            if value.amount < 0:
                raise ValueError(f"ComputationLine.{name} must be >= 0, got {value}")

    @property
    def net_proceeds_gbp(self) -> Money:
        """C — proceeds after incidental costs of disposal."""
        with exact_arithmetic():
            return self.gross_proceeds_gbp - self.disposal_costs_gbp

    @property
    def allowable_costs_gbp(self) -> Money:
        """G — purchase price plus incidental costs of acquisition."""
        with exact_arithmetic():
            return self.cost_gbp + self.acquisition_costs_gbp

    @property
    def gain_gbp(self) -> Money:
        """H — the gain (positive) or loss (negative) on this line."""
        with exact_arithmetic():
            return self.net_proceeds_gbp - self.allowable_costs_gbp


@dataclass(frozen=True, slots=True, kw_only=True)
class DisposalComputation:
    """One HMRC disposal: every line for one instrument on one day.

    The notes count "all disposals of the same class of share or
    security in the same company made on the same day as a single
    disposal", and the report applies the same grouping to every asset
    class. The totals are sums over the lines — derived, not stored.

    Attributes:
        instrument: The asset disposed of (an FX pool's synthetic
            instrument for currency disposals).
        disposal_date: The day.
        lines: The identifications, in the engine's rule order.
    """

    instrument: AnyInstrument
    disposal_date: date
    lines: tuple[ComputationLine, ...]

    def __post_init__(self) -> None:
        """A disposal without lines is not a disposal."""
        if not self.lines:
            raise ValueError("DisposalComputation.lines must not be empty")

    @property
    def asset_class(self) -> AssetClass:
        """The instrument's class — what decides the section."""
        return self.instrument.asset_class

    @property
    def section(self) -> Sa108SectionKind:
        """The SA108 section this disposal is reported in."""
        return Sa108SectionKind.for_asset_class(self.asset_class)

    @property
    def disposal_refs(self) -> tuple[EventRef, ...]:
        """The distinct disposal events behind the lines, first-seen order."""
        seen: dict[int, EventRef] = {}
        for line in self.lines:
            seen.setdefault(line.disposal.event_id, line.disposal)
        return tuple(seen.values())

    @property
    def matched_quantity(self) -> Decimal:
        """Units disposed of across every line."""
        return sum((line.matched_quantity for line in self.lines), Decimal(0))

    @property
    def gross_proceeds_gbp(self) -> Money:
        """A over the lines."""
        return _sum_money(line.gross_proceeds_gbp for line in self.lines)

    @property
    def disposal_costs_gbp(self) -> Money:
        """B over the lines."""
        return _sum_money(line.disposal_costs_gbp for line in self.lines)

    @property
    def net_proceeds_gbp(self) -> Money:
        """C over the lines."""
        return _sum_money(line.net_proceeds_gbp for line in self.lines)

    @property
    def allowable_costs_gbp(self) -> Money:
        """G over the lines."""
        return _sum_money(line.allowable_costs_gbp for line in self.lines)

    @property
    def gain_gbp(self) -> Money:
        """H over the lines — may net a gain on one line against a loss on another."""
        return _sum_money(line.gain_gbp for line in self.lines)


def _sum_money(amounts: Iterable[Money]) -> Money:
    """Sum GBP amounts exactly, starting from zero so an empty iterable is still valid."""
    with exact_arithmetic():
        total = Money.zero("GBP")
        for amount in amounts:
            total = total + amount
        return total


# ---------------------------------------------------------------------------
# The report
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class RunHeader:
    """Which persisted run the report was built from."""

    run_id: int
    computed_at: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class Sa108Report:
    """The whole document: box figures per section, year totals, computations, issues.

    Attributes:
        tax_year: The year reported.
        run: The persisted run the figures come from.
        sections: Both SA108 sections, always present, in form order.
        totals: The two sections added together.
        disposals: Every computation, ordered section → asset class →
            date → symbol.
        issues: The run's errors and warnings — what the figures do
            not include.
    """

    tax_year: TaxYear
    run: RunHeader
    sections: tuple[Sa108Section, ...]
    totals: Sa108Figures
    disposals: tuple[DisposalComputation, ...]
    issues: tuple[RunIssue, ...]

    def __post_init__(self) -> None:
        """Exactly the two sections in form order, totals that add up, dates inside the year."""
        kinds = tuple(section.kind for section in self.sections)
        if kinds != tuple(Sa108SectionKind):
            raise ValueError(
                f"Sa108Report.sections must be exactly {[k.value for k in Sa108SectionKind]} "
                f"in that order, got {[k.value for k in kinds]}"
            )
        total = Sa108Figures.zero()
        for section in self.sections:
            total = total + section.figures
        if total != self.totals:
            raise ValueError("Sa108Report.totals must equal the sum of the section figures")
        for disposal in self.disposals:
            if not self.tax_year.contains(disposal.disposal_date):
                raise ValueError(
                    f"Sa108Report: {disposal.instrument.symbol} disposal on "
                    f"{disposal.disposal_date} is outside {self.tax_year.label}"
                )

    def section_for(self, kind: Sa108SectionKind) -> Sa108Section:
        """The section of the given kind (both always exist)."""
        for section in self.sections:
            if section.kind is kind:
                return section
        raise KeyError(kind)  # unreachable after __post_init__; keeps mypy honest

    def disposals_in(self, kind: Sa108SectionKind) -> tuple[DisposalComputation, ...]:
        """The computations reported in one section, in report order."""
        return tuple(d for d in self.disposals if d.section is kind)

    @property
    def errors(self) -> tuple[RunIssue, ...]:
        """The error-severity issues — the ones that make the figures incomplete."""
        return tuple(i for i in self.issues if i.severity is IssueSeverity.ERROR)

    @property
    def warnings(self) -> tuple[RunIssue, ...]:
        """The warning-severity issues — notices about what the figures leave out."""
        return tuple(i for i in self.issues if i.severity is IssueSeverity.WARNING)

    @property
    def is_complete(self) -> bool:
        """True when nothing failed during the run the figures come from."""
        return not self.errors


__all__ = [
    "AssetClassFigures",
    "BoxNumbers",
    "CloseOutBasis",
    "ComputationLine",
    "DirectBasis",
    "DisposalComputation",
    "EventRef",
    "GrantBasis",
    "GrantCloseRef",
    "LineBasis",
    "PoolBasis",
    "RunHeader",
    "Sa108Figures",
    "Sa108Report",
    "Sa108Section",
    "Sa108SectionKind",
    "exact_arithmetic",
]
