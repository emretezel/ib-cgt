"""Money and CurrencyPair value objects.

UK CGT requires every disposal to be reported in GBP, but the calculator
ingests trades in the instrument's native currency and converts later via
the FX service. That means the domain layer must distinguish native-ccy
monetary amounts from GBP amounts *at the type level* — a `Money` that
carries its own currency tag is the lightest way to achieve that without
introducing per-currency subclasses.

Design notes:

* `amount` is always `Decimal` — never `float`. Floats are banned for
  financial arithmetic because binary rounding produces demonstrably
  wrong pennies. The constructor accepts `str | int | Decimal` to make
  tests and REPL use ergonomic, but silently rejects `float`.
* `currency` is the ISO-4217 3-letter code, upper-case, validated on
  construction. The `CurrencyPair` helper formalises the FX-asset-class
  convention of "base vs quote", with GBP as the quote in every UK-CGT
  context.
* Arithmetic is only defined for same-currency operands; a mismatch
  raises `CurrencyMismatchError` rather than silently coercing. Scalar
  multiplication (`Money * Decimal`) is supported because cost basis =
  `price * quantity` is a ubiquitous operation in the matching engine.
  Multiplying two `Money` values, on the other hand, is nonsense — the
  result would have unit `currency^2` — and is deliberately not defined.
* Amounts are stored with whatever precision they came in with. No
  internal quantisation: reporting is responsible for formatting to
  pennies when it renders, not the domain type. Python's default
  `decimal` context (`prec=28`) gives ample headroom.

Author: Emre Tezel
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from typing import Final

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class CurrencyMismatchError(ValueError):
    """Raised when two Money values with different currencies are combined.

    Inherits from `ValueError` so generic input-validation catches still
    work; domain-aware code can catch the narrower type.
    """


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# Strict ISO-4217 shape: exactly three ASCII upper-case letters. We do not
# validate against the full ISO-4217 registry — IB reports may include codes
# that pre-date / post-date our local list, and downstream FX service calls
# will fail loudly on unknown codes anyway.
_CURRENCY_CODE_PATTERN: Final = re.compile(r"^[A-Z]{3}$")


def validate_currency_code(code: str) -> str:
    """Return `code` unchanged if it is a valid ISO-4217 shape; else raise.

    Exported so sibling modules in the domain layer (notably `trading.py`
    for instrument/currency consistency) can reuse the same rule without
    reaching for a private symbol.
    """
    if not isinstance(code, str) or not _CURRENCY_CODE_PATTERN.fullmatch(code):
        raise ValueError(f"currency must be a 3-letter upper-case ISO-4217 code, got {code!r}")
    return code


def _coerce_amount(value: Decimal | int | str) -> Decimal:
    """Coerce supported amount inputs to `Decimal`; reject `float`.

    We deliberately accept `str` and `int` for ergonomic test / REPL use
    (`Money.gbp("10.00")`, `Money.gbp(0)`), but reject `float` because
    binary-float rounding corrupts monetary amounts silently.
    """
    if isinstance(value, bool):
        # `bool` is a subclass of `int`; we don't want `True` sneaking in
        # as amount 1.
        raise TypeError("Money.amount cannot be bool")
    if isinstance(value, float):
        raise TypeError(
            "Money.amount must not be float — use Decimal, int, or str to "
            "avoid binary-float rounding errors"
        )
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, str):
        return Decimal(value)
    raise TypeError(f"Money.amount must be Decimal | int | str, got {type(value).__name__}")


# ---------------------------------------------------------------------------
# Money
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Money:
    """An immutable monetary amount tagged with its ISO-4217 currency.

    Attributes:
        amount: The monetary amount. Always `Decimal`; may be negative
            (realised losses, fee rebates computed elsewhere, etc.).
        currency: The ISO-4217 3-letter currency code in upper case.
    """

    amount: Decimal
    currency: str

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def __post_init__(self) -> None:
        """Validate that `amount` is a `Decimal` and `currency` is ISO-4217."""
        # The factory methods coerce inputs before calling `__init__`, but a
        # direct `Money(Decimal("1"), "gbp")` construction still needs to be
        # caught — guard the fields here rather than relying on the factories.
        if not isinstance(self.amount, Decimal):
            raise TypeError(f"Money.amount must be Decimal, got {type(self.amount).__name__}")
        validate_currency_code(self.currency)

    # ------------------------------------------------------------------
    # Factories
    # ------------------------------------------------------------------

    @classmethod
    def of(cls, amount: Decimal | int | str, currency: str) -> Money:
        """Construct a Money with ergonomic amount/currency inputs."""
        return cls(_coerce_amount(amount), validate_currency_code(currency))

    @classmethod
    def gbp(cls, amount: Decimal | int | str) -> Money:
        """Shorthand for GBP-denominated Money.

        GBP is the reporting currency for UK CGT, so it shows up heavily
        in tests and in the derived (post-conversion) domain shapes.
        """
        return cls.of(amount, "GBP")

    @classmethod
    def zero(cls, currency: str) -> Money:
        """Return a zero-amount Money of the given currency."""
        return cls(Decimal(0), validate_currency_code(currency))

    # ------------------------------------------------------------------
    # Arithmetic
    # ------------------------------------------------------------------

    def _check_same_currency(self, other: Money) -> None:
        """Raise `CurrencyMismatchError` unless `other` shares this currency."""
        if self.currency != other.currency:
            raise CurrencyMismatchError(f"cannot combine {self.currency} and {other.currency}")

    def __add__(self, other: Money) -> Money:
        """Sum two same-currency amounts."""
        self._check_same_currency(other)
        return Money(self.amount + other.amount, self.currency)

    def __sub__(self, other: Money) -> Money:
        """Subtract two same-currency amounts."""
        self._check_same_currency(other)
        return Money(self.amount - other.amount, self.currency)

    def __neg__(self) -> Money:
        """Return the additive inverse."""
        return Money(-self.amount, self.currency)

    def __mul__(self, factor: Decimal | int) -> Money:
        """Scale by a dimensionless factor; `Money * Money` is undefined.

        `float` factors are rejected for the same reason `float` amounts
        are — silent binary-rounding corruption.
        """
        if isinstance(factor, bool):
            raise TypeError("Money.__mul__ factor cannot be bool")
        if isinstance(factor, float):
            raise TypeError("Money.__mul__ factor must not be float")
        if isinstance(factor, int):
            return Money(self.amount * Decimal(factor), self.currency)
        if isinstance(factor, Decimal):
            return Money(self.amount * factor, self.currency)
        raise TypeError(
            f"Money can only be multiplied by Decimal or int, got {type(factor).__name__}"
        )

    __rmul__ = __mul__

    # ------------------------------------------------------------------
    # Predicates and presentation
    # ------------------------------------------------------------------

    def is_gbp(self) -> bool:
        """True iff this amount is denominated in GBP."""
        return self.currency == "GBP"

    def __repr__(self) -> str:
        """Render as 'CCY 1,234.56' — compact, human-readable in REPL/Jupyter."""
        # Format with thousands separators; keep the amount's natural
        # precision (no forced 2dp) so debug output reflects reality.
        return f"{self.currency} {self.amount:,}"


# ---------------------------------------------------------------------------
# CurrencyPair (FX asset class)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CurrencyPair:
    """An ordered (base, quote) pair of currencies.

    For UK CGT purposes we only ever pool non-GBP currencies against GBP,
    so in every production code path `quote == "GBP"`. The type permits
    arbitrary quote currencies because (a) generality is free here and
    (b) tests and future use cases (e.g. cross-rate computations) may
    need it.
    """

    base: str
    quote: str

    def __post_init__(self) -> None:
        """Validate both legs as ISO-4217 codes; reject degenerate pairs."""
        validate_currency_code(self.base)
        validate_currency_code(self.quote)
        if self.base == self.quote:
            raise ValueError(
                f"CurrencyPair base and quote must differ, got {self.base}/{self.quote}"
            )

    def __repr__(self) -> str:
        """Render as the conventional concatenation, e.g. 'EURGBP'."""
        return f"{self.base}{self.quote}"
