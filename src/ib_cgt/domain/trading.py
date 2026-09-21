"""Raw (native-currency) trading value objects.

This module models the *input* side of the pipeline: accounts, the
discriminated `Instrument` hierarchy (one subclass per asset class), and
the `Trade` record that ingestion produces from an IB statement.

Raw shapes live in this module; their GBP-converted derived counterparts
(`Acquisition`, `Disposal`, `MatchedDisposal`, …) live in `disposal.py`.
Keeping them apart is a deliberate design choice — `docs/architecture.md
§Component map, item 1` says "Raw (native-currency) shapes separate from
derived (GBP) shapes" — and makes it impossible to accidentally mix a
pre-conversion Trade into a post-conversion matching calculation.

Why a sealed hierarchy and not a flat `Instrument` with optional fields?
Different asset classes have *structurally* different metadata: a future
has a contract multiplier and an expiry, an FX holding has a currency
pair, a bond may be CGT-exempt, a stock has none of the above. Encoding
that as nullable fields on one class forces every rule engine to assert
"the multiplier is not None" before using it — defeating `--strict` mypy
and obscuring which fields are valid for which class. A sealed hierarchy
lets the rule engines dispatch via `match instrument: case
FutureInstrument(...): ...` and get exhaustive type-narrowing for free.

The same argument gives each class its own natural key rather than a
shared optional one: stocks and futures carry IB's `conid`, bonds the
ISIN, FX pairs the currency pair. Those keys exist so ingestion can
recognise the same instrument across statements (IB renames symbols;
it never changes a conid or an ISIN). Once persisted, the surrogate
`instruments.instrument_id` is the only identity the calculator uses.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from typing import ClassVar, Final
from zoneinfo import ZoneInfo

from ib_cgt.domain.enums import AssetClass, TradeAction
from ib_cgt.domain.money import CurrencyPair, Money, validate_currency_code

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class InvalidInstrumentError(ValueError):
    """Raised when an Instrument subclass is constructed with bad fields."""


class InvalidTradeError(ValueError):
    """Raised when a Trade is constructed in a state forbidden by CGT rules."""


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# UK tax law interprets "date of acquisition / disposal" as the date on the
# contract note, which for a UK taxpayer is the UK-local date. We therefore
# project trade_datetime (stored UTC for audit) into Europe/London when
# asserting it matches trade_date.
_UK_ZONE: Final = ZoneInfo("Europe/London")

# Actions that only make sense for non-future asset classes.
_NON_FUTURE_ACTIONS: Final = frozenset({TradeAction.BUY, TradeAction.SELL})

# Actions that only make sense on a FutureInstrument.
_FUTURE_ACTIONS: Final = frozenset(
    {
        TradeAction.OPEN_LONG,
        TradeAction.CLOSE_LONG,
        TradeAction.OPEN_SHORT,
        TradeAction.CLOSE_SHORT,
    }
)


# ---------------------------------------------------------------------------
# Account
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Account:
    """An Interactive Brokers account belonging to a single UK taxpayer.

    S.104 pools span all of the taxpayer's accounts (per the scope
    decision in `docs/architecture.md §Scope — Accounts`), but trades
    still carry their originating account ID so reports can attribute
    realised gains back to the right broker statement.

    Attributes:
        account_id: The IB account code as it appears on the statement
            (e.g. `"U1234567"`).
        label: Optional human-friendly label ("ISA", "joint", …).
    """

    account_id: str
    label: str | None = None

    def __post_init__(self) -> None:
        """Reject empty account identifiers."""
        if not self.account_id or not self.account_id.strip():
            raise ValueError("Account.account_id must be a non-empty string")


# ---------------------------------------------------------------------------
# Instrument hierarchy
# ---------------------------------------------------------------------------


def _validate_conid(conid: object, *, owner: str) -> None:
    """Reject anything but a strictly positive `int` as an IB contract id.

    Shared by `StockInstrument` and `FutureInstrument`. `bool` is a
    subclass of `int` in Python, so `True` would otherwise slip through
    as conid `1` — it is rejected explicitly. The check is written
    against `object` so a mis-typed call site (a conid still in its raw
    statement-text form, for example) fails here rather than at the
    STRICT `INTEGER` column.
    """
    if isinstance(conid, bool) or not isinstance(conid, int):
        raise InvalidInstrumentError(
            f"{owner}.conid must be an int, got {type(conid).__name__}: {conid!r}"
        )
    if conid <= 0:
        raise InvalidInstrumentError(f"{owner}.conid must be > 0, got {conid}")


@dataclass(frozen=True, slots=True, kw_only=True)
class Instrument:
    """Shared fields of every tradable instrument.

    Not intended to be constructed directly — use one of the concrete
    subclasses. We keep the base class abstract-by-convention (no ABC
    boilerplate) because the real enforcement happens via the
    `AnyInstrument` union: downstream code receives one of four known
    subclasses and never a bare `Instrument`.

    Identity is *not* a shared field: each subclass carries its own
    natural key (`conid` for stocks and futures, `isin` for bonds, the
    currency pair for FX) and the persistence layer maps that to the
    surrogate `instruments.instrument_id`, which is the only identity
    the calculator ever compares. `symbol` is display text — IB renames
    symbols between statements, so nothing may treat it as identity.

    Attributes:
        symbol: The IB symbol or ticker as it appears in statements.
        currency: The currency IB prices the instrument's trades and
            positions in (ISO-4217). For a stock this is the trade
            currency of the listing, which is not necessarily the
            currency its distributions are paid in.
    """

    symbol: str
    currency: str

    # Each subclass sets this to its asset-class discriminator. Declared on
    # the base (without a value) so a `type: ignore`-free `instrument.asset_class`
    # works on any `AnyInstrument` — every subclass promises a concrete value.
    asset_class: ClassVar[AssetClass]

    def __post_init__(self) -> None:
        """Validate fields shared by every subclass."""
        if not self.symbol or not self.symbol.strip():
            raise InvalidInstrumentError("Instrument.symbol must be non-empty")
        # Reuse the currency-code validator from money.py rather than duplicating;
        # money.py has no domain-internal dependencies, so no import cycle risk.
        validate_currency_code(self.currency)


@dataclass(frozen=True, slots=True, kw_only=True)
class StockInstrument(Instrument):
    """An equity listing, identified by IB's contract id.

    Attributes:
        conid: IB's contract id for the listing — stable across
            statements even when IB renames the symbol (`JNKEz` became
            `JNKE` under conid 102048570). One conid is one listing on
            one venue in one trade currency.
    """

    conid: int
    asset_class: ClassVar[AssetClass] = AssetClass.STOCK

    def __post_init__(self) -> None:
        """Validate the shared fields, then the conid."""
        # See FutureInstrument.__post_init__ for why the explicit-class
        # form of `super()` is required under `@dataclass(slots=True)`.
        super(StockInstrument, self).__post_init__()
        _validate_conid(self.conid, owner="StockInstrument")


@dataclass(frozen=True, slots=True, kw_only=True)
class BondInstrument(Instrument):
    """A bond holding, identified by ISIN, with the CGT-exempt flag.

    UK gilts and Qualifying Corporate Bonds are CGT-exempt; setting
    `is_cgt_exempt=True` tells the `BondRuleEngine` to skip the matching
    logic entirely for this instrument.

    Attributes:
        isin: The bond's ISIN — its natural key (migration 014). IB
            prints it in the bonds-shaped Financial Instrument
            Information table and inline in every maturity row, so it
            is always available and always required.
        is_cgt_exempt: True for gilts / QCBs.
    """

    isin: str
    is_cgt_exempt: bool = False
    asset_class: ClassVar[AssetClass] = AssetClass.BOND

    def __post_init__(self) -> None:
        """Validate the shared fields, then the ISIN."""
        super(BondInstrument, self).__post_init__()
        if not self.isin or not self.isin.strip():
            raise InvalidInstrumentError("BondInstrument.isin must be non-empty")


@dataclass(frozen=True, slots=True, kw_only=True)
class FutureInstrument(Instrument):
    """A futures contract, identified by IB's contract id.

    Individual-investor UK CGT treats each closed contract as its own
    disposal, so the `FutureRuleEngine` needs the multiplier to compute
    notional values and the expiry to distinguish rolled vs expired
    positions.

    Attributes:
        conid: IB's contract id — one conid is one contract (one root,
            one expiry, one venue), so two delivery months of the same
            root are two conids.
        contract_multiplier: Notional per point, strictly positive.
        expiry_date: Last trading / delivery date.
    """

    conid: int
    contract_multiplier: Decimal
    expiry_date: date
    asset_class: ClassVar[AssetClass] = AssetClass.FUTURE

    def __post_init__(self) -> None:
        """Validate the conid and that the multiplier is strictly positive."""
        # `@dataclass(slots=True)` replaces the class object after this method
        # is compiled, which breaks the zero-arg `super()` closure's
        # `__class__` cell. Calling with the explicit class resolves the
        # post-decoration class at call time.
        super(FutureInstrument, self).__post_init__()
        _validate_conid(self.conid, owner="FutureInstrument")
        if self.contract_multiplier <= 0:
            raise InvalidInstrumentError(
                f"FutureInstrument.contract_multiplier must be > 0, got {self.contract_multiplier}"
            )


@dataclass(frozen=True, slots=True, kw_only=True)
class FXInstrument(Instrument):
    """An FX holding treated as its own CGT asset class.

    Per `docs/architecture.md §Scope — FX treatment`, FX is pooled per
    currency pair vs GBP, not merely converted. The `currency_pair`
    field identifies which pool this instrument belongs in.
    """

    currency_pair: CurrencyPair
    asset_class: ClassVar[AssetClass] = AssetClass.FX

    def __post_init__(self) -> None:
        """Validate that the pair's base matches the instrument's currency."""
        # See FutureInstrument.__post_init__ — `@dataclass(slots=True)` replaces
        # the class object, so zero-arg `super()` can't find the right class.
        super(FXInstrument, self).__post_init__()
        # For UK taxpayers, FX holdings are held in the *base* currency and
        # valued against GBP. The instrument's trading currency should match
        # the pair's base — otherwise the pair and the instrument disagree.
        if self.currency_pair.base != self.currency:
            raise InvalidInstrumentError(
                f"FXInstrument.currency_pair.base ({self.currency_pair.base}) "
                f"must match currency ({self.currency})"
            )


# Discriminated union of concrete instrument types. Rule engines should
# accept this type, never the bare `Instrument` base.
AnyInstrument = StockInstrument | BondInstrument | FutureInstrument | FXInstrument


# ---------------------------------------------------------------------------
# Trade (raw, native-currency)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, kw_only=True)
class Trade:
    """A single executed trade as ingested from an IB statement.

    All amounts are in the instrument's native currency; conversion to
    GBP happens later in the calculator via the FX service. Keeping
    native-currency amounts here is what makes the derived GBP shapes
    in `disposal.py` distinguishable by type.

    Attributes:
        account_id: IB account this trade executed against.
        instrument: One of the four concrete `*Instrument` subclasses.
        action: Direction (stocks/bonds/FX) or open/close-long/short
            (futures). See `TradeAction`.
        trade_datetime: Timezone-aware execution timestamp; persisted in
            UTC for forensic audit.
        trade_date: The UK-local-date projection of `trade_datetime`,
            used for same-day grouping by the matching engine. Carried
            explicitly so downstream code never has to re-derive it.
        settlement_date: Settlement (T+N) date as reported by IB.
        quantity: Strictly positive magnitude; the sign/side is encoded
            in `action`, not here.
        price: Per-unit execution price, in the instrument's currency.
        fees: Total fees charged on this trade, same currency as `price`.
            Must be >= 0; fee rebates are modelled separately when the
            ingestion layer needs them, never as negative fees.
        accrued_interest: Purchase/sale accrued interest — bonds only.
            Adjusts cost basis / proceeds per UK CGT bond rules.
    """

    account_id: str
    instrument: AnyInstrument
    action: TradeAction
    trade_datetime: datetime
    trade_date: date
    settlement_date: date
    quantity: Decimal
    price: Money
    fees: Money
    accrued_interest: Money | None = None

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def __post_init__(self) -> None:
        """Enforce every invariant documented in the class docstring."""
        self._check_identifiers()
        self._check_quantity()
        self._check_datetime_fields()
        self._check_monetary_currencies()
        self._check_action_vs_instrument()
        self._check_accrued_interest()

    def _check_identifiers(self) -> None:
        """`account_id` must be a non-empty string."""
        if not self.account_id or not self.account_id.strip():
            raise InvalidTradeError("Trade.account_id must be a non-empty string")

    def _check_quantity(self) -> None:
        """`quantity` is strictly positive; sign is carried by `action`."""
        if not isinstance(self.quantity, Decimal):
            raise InvalidTradeError(
                f"Trade.quantity must be Decimal, got {type(self.quantity).__name__}"
            )
        if self.quantity <= 0:
            raise InvalidTradeError(f"Trade.quantity must be > 0, got {self.quantity}")

    def _check_datetime_fields(self) -> None:
        """`trade_datetime` must be tz-aware and project to `trade_date` in UK-local."""
        if self.trade_datetime.tzinfo is None:
            raise InvalidTradeError(
                "Trade.trade_datetime must be timezone-aware; naive datetimes "
                "are ambiguous and cannot be projected to a UK-local date"
            )
        expected_uk_date = self.trade_datetime.astimezone(_UK_ZONE).date()
        if self.trade_date != expected_uk_date:
            raise InvalidTradeError(
                f"Trade.trade_date ({self.trade_date}) does not match the "
                f"UK-local projection of trade_datetime ({expected_uk_date})"
            )

    def _check_monetary_currencies(self) -> None:
        """`price` and `fees` currencies must agree with the IB convention.

        `price.currency` always matches `instrument.currency` — IB quotes
        the per-unit price in the trade's native currency for every asset
        class (for FX, that is the pair's base, which `FXInstrument`
        already forces to equal `currency`).

        `fees.currency` follows the asset class. IB denominates the
        Comm/Fee column in the trade's native currency for stocks, bonds,
        and futures; for **forex** rows it is **always GBP**, regardless
        of the pair traded. The branching here encodes that convention as
        a typed invariant.
        """
        if self.price.currency != self.instrument.currency:
            raise InvalidTradeError(
                f"Trade.price currency ({self.price.currency}) must match "
                f"instrument currency ({self.instrument.currency})"
            )
        if isinstance(self.instrument, FXInstrument):
            if self.fees.currency != "GBP":
                raise InvalidTradeError(
                    f"Trade.fees currency ({self.fees.currency}) for an FX "
                    "trade must be GBP (IB denominates forex commissions in "
                    "GBP regardless of the pair)"
                )
        elif self.fees.currency != self.instrument.currency:
            raise InvalidTradeError(
                f"Trade.fees currency ({self.fees.currency}) must match "
                f"instrument currency ({self.instrument.currency})"
            )
        if self.fees.amount < 0:
            raise InvalidTradeError(
                f"Trade.fees must be >= 0, got {self.fees.amount}; rebates "
                "should not be modelled as negative fees"
            )

    def _check_action_vs_instrument(self) -> None:
        """BUY/SELL only for non-futures; OPEN_*/CLOSE_* only for futures."""
        is_future = isinstance(self.instrument, FutureInstrument)
        if is_future and self.action not in _FUTURE_ACTIONS:
            raise InvalidTradeError(
                f"FutureInstrument requires OPEN_*/CLOSE_* action, got {self.action}"
            )
        if not is_future and self.action not in _NON_FUTURE_ACTIONS:
            raise InvalidTradeError(
                f"Non-future instruments require BUY/SELL action, got {self.action}"
            )

    def _check_accrued_interest(self) -> None:
        """Accrued interest is bonds-only and must share the trading currency."""
        if self.accrued_interest is None:
            return
        if not isinstance(self.instrument, BondInstrument):
            raise InvalidTradeError(
                "Trade.accrued_interest is only valid for BondInstrument trades"
            )
        if self.accrued_interest.currency != self.instrument.currency:
            raise InvalidTradeError(
                f"Trade.accrued_interest currency ({self.accrued_interest.currency}) "
                f"must match instrument currency ({self.instrument.currency})"
            )

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    @staticmethod
    def uk_date_of(trade_datetime: datetime) -> date:
        """Project a tz-aware datetime into the UK-local date.

        Exposed so ingestion code can produce a Trade without duplicating
        the Europe/London conversion logic.
        """
        if trade_datetime.tzinfo is None:
            raise ValueError("uk_date_of() requires a timezone-aware datetime")
        return trade_datetime.astimezone(_UK_ZONE).date()
