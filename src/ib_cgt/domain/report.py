"""Tax-year report value objects.

The reporting component (`ib_cgt.report`) consumes a `TaxYearReport` and
renders it to console tables, CSV, and JSON. This module only defines
the *shape* of the report — all formatting lives downstream.

Why tuples instead of lists? Frozen dataclasses with mutable-sequence
fields aren't actually immutable: `report.matched_disposals.append(...)`
would succeed at runtime. Tuples preserve the immutability guarantee
and cost nothing in ergonomics (iteration, `len(...)`, `[i]`, unpacking
all work identically).

Author: Emre Tezel
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from ib_cgt.domain.disposal import FutureRealisation, MatchedDisposal
from ib_cgt.domain.enums import AssetClass
from ib_cgt.domain.money import Money
from ib_cgt.domain.tax_year import TaxYear


@dataclass(frozen=True, slots=True, kw_only=True)
class AssetClassSummary:
    """Aggregated totals for one asset class within a tax year.

    `total_gains_gbp` and `total_losses_gbp` are both non-negative by
    convention — gains sum up positive outcomes, losses sum up the
    *absolute value* of negative outcomes. `net_gbp` is
    `total_gains_gbp - total_losses_gbp` (can be negative).

    Attributes:
        asset_class: Which asset class this summary covers.
        disposal_count: Number of `MatchedDisposal` rows rolled up here.
        total_proceeds_gbp: Sum of matched proceeds across all rows.
        total_cost_gbp: Sum of matched cost across all rows.
        total_gains_gbp: Sum of positive outcomes (gains).
        total_losses_gbp: Sum of the magnitudes of negative outcomes.
        net_gbp: Net result for the asset class.
    """

    asset_class: AssetClass
    disposal_count: int
    total_proceeds_gbp: Money
    total_cost_gbp: Money
    total_gains_gbp: Money
    total_losses_gbp: Money
    net_gbp: Money

    def __post_init__(self) -> None:
        """Every monetary field must be GBP; `disposal_count` non-negative."""
        if self.disposal_count < 0:
            raise ValueError(
                f"AssetClassSummary.disposal_count must be >= 0, got {self.disposal_count}"
            )
        for label, value in (
            ("total_proceeds_gbp", self.total_proceeds_gbp),
            ("total_cost_gbp", self.total_cost_gbp),
            ("total_gains_gbp", self.total_gains_gbp),
            ("total_losses_gbp", self.total_losses_gbp),
            ("net_gbp", self.net_gbp),
        ):
            if not value.is_gbp():
                raise ValueError(f"AssetClassSummary.{label} must be GBP, got {value.currency}")
        # By the convention documented above, gains and losses are stored as
        # non-negative magnitudes. Catching the sign here avoids a whole class
        # of "my net gain is negative because I stored losses as negatives
        # twice" bugs in downstream renderers.
        if self.total_gains_gbp.amount < 0:
            raise ValueError(
                f"AssetClassSummary.total_gains_gbp must be >= 0, got {self.total_gains_gbp.amount}"
            )
        if self.total_losses_gbp.amount < 0:
            raise ValueError(
                f"AssetClassSummary.total_losses_gbp must be >= 0, "
                f"got {self.total_losses_gbp.amount}"
            )


@dataclass(frozen=True, slots=True, kw_only=True)
class TaxYearReport:
    """Everything the reporting layer needs for one tax year.

    Attributes:
        tax_year: The tax year being reported.
        matched_disposals: Every `MatchedDisposal` that fell into this
            tax year, in the order the matching engine emitted them.
        summaries: Per-asset-class rollups derived from
            `matched_disposals` and `future_realisations`; the
            calculator computes these once (`build`) so every renderer
            reads the same totals.
        future_realisations: Every closed-out futures contract whose
            `close_date` fell into this tax year. Futures do not fit
            `MatchedDisposal` (see `FutureRealisation`), so they sit
            beside the chunks rather than among them.
    """

    tax_year: TaxYear
    matched_disposals: tuple[MatchedDisposal, ...]
    summaries: tuple[AssetClassSummary, ...]
    future_realisations: tuple[FutureRealisation, ...] = ()

    def __post_init__(self) -> None:
        """Summaries must not double-up; every row must fall inside the year."""
        seen: set[AssetClass] = set()
        for summary in self.summaries:
            if summary.asset_class in seen:
                raise ValueError(
                    f"TaxYearReport.summaries contains duplicate asset class {summary.asset_class}"
                )
            seen.add(summary.asset_class)
        # The tax-year filter is the calculator's one job that the
        # renderers cannot double-check, so the report refuses a row
        # from the wrong year outright.
        for chunk in self.matched_disposals:
            if not self.tax_year.contains(chunk.disposal_date):
                raise ValueError(
                    f"TaxYearReport: disposal #{chunk.disposal_trade_id} dated "
                    f"{chunk.disposal_date} is outside {self.tax_year.label}"
                )
        for realisation in self.future_realisations:
            if not self.tax_year.contains(realisation.close_date):
                raise ValueError(
                    f"TaxYearReport: futures realisation #{realisation.open_trade_id}->"
                    f"#{realisation.close_trade_id} closed {realisation.close_date} is "
                    f"outside {self.tax_year.label}"
                )

    @classmethod
    def build(
        cls,
        tax_year: TaxYear,
        matched_disposals: Iterable[MatchedDisposal],
        future_realisations: Iterable[FutureRealisation] = (),
    ) -> TaxYearReport:
        """Assemble a report, deriving one summary per asset class with rows.

        Chunks roll up by their instrument's asset class; futures
        realisations roll up under `AssetClass.FUTURE`, counting one
        realisation per row, with `proceeds_gbp` (signed) and
        `cost_gbp` summed and the gain split into its positive and
        negative parts exactly as chunks are. Classes with no rows get
        no summary. Summaries come out in `AssetClass` declaration
        order so renderers print a stable table.
        """
        chunks = tuple(matched_disposals)
        realisations = tuple(future_realisations)
        totals: dict[AssetClass, _Totals] = {}
        for chunk in chunks:
            totals.setdefault(chunk.instrument.asset_class, _Totals()).add(
                chunk.matched_proceeds_gbp, chunk.matched_cost_gbp
            )
        for realisation in realisations:
            totals.setdefault(AssetClass.FUTURE, _Totals()).add(
                realisation.proceeds_gbp, realisation.cost_gbp
            )
        summaries = tuple(
            totals[asset_class].summary(asset_class)
            for asset_class in AssetClass
            if asset_class in totals
        )
        return cls(
            tax_year=tax_year,
            matched_disposals=chunks,
            summaries=summaries,
            future_realisations=realisations,
        )

    @property
    def net_gbp(self) -> Money:
        """Net gain (or loss) across every asset class in this report."""
        # Start from zero GBP so a report with no summaries still produces a
        # valid Money value rather than raising.
        total = Money.zero("GBP")
        for summary in self.summaries:
            total = total + summary.net_gbp
        return total

    @property
    def is_empty(self) -> bool:
        """True when the year has neither disposal chunks nor futures realisations."""
        return not self.matched_disposals and not self.future_realisations

    def summary_for(self, asset_class: AssetClass) -> AssetClassSummary | None:
        """Return the summary for `asset_class`, or `None` if absent.

        Reports may omit asset classes with zero disposals; renderers
        must handle the missing case gracefully, hence the `Optional`
        return.
        """
        for summary in self.summaries:
            if summary.asset_class is asset_class:
                return summary
        return None


class _Totals:
    """Running GBP totals for one asset class while `TaxYearReport.build` walks the rows.

    Mutable on purpose — it exists for the duration of one `build`
    call and is turned into a frozen `AssetClassSummary` at the end.
    """

    def __init__(self) -> None:
        """Start every total at zero GBP."""
        self.count = 0
        self.proceeds = Money.zero("GBP")
        self.cost = Money.zero("GBP")
        self.gains = Money.zero("GBP")
        self.losses = Money.zero("GBP")

    def add(self, proceeds_gbp: Money, cost_gbp: Money) -> None:
        """Fold one row in, splitting its outcome into gain or loss magnitude."""
        self.count += 1
        self.proceeds = self.proceeds + proceeds_gbp
        self.cost = self.cost + cost_gbp
        outcome = proceeds_gbp.amount - cost_gbp.amount
        if outcome >= 0:
            self.gains = self.gains + Money.gbp(outcome)
        else:
            self.losses = self.losses + Money.gbp(-outcome)

    def summary(self, asset_class: AssetClass) -> AssetClassSummary:
        """Freeze the totals into the report's summary shape."""
        return AssetClassSummary(
            asset_class=asset_class,
            disposal_count=self.count,
            total_proceeds_gbp=self.proceeds,
            total_cost_gbp=self.cost,
            total_gains_gbp=self.gains,
            total_losses_gbp=self.losses,
            net_gbp=Money.gbp(self.gains.amount - self.losses.amount),
        )


__all__ = ["AssetClassSummary", "TaxYearReport"]
