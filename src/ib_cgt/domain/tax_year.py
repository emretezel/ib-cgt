"""UK tax year value object.

The UK tax year runs 6 April → 5 April of the following calendar year.
Getting the boundary right matters: a disposal on 5 April 2025 belongs in
the 2024/25 tax year, while a disposal on 6 April 2025 belongs in 2025/26.
The matching engine, the tax-year filter in the calculator, and the
reporting layer all lean on this type for that decision.

We encode the tax year with a single integer `start_year` so that equality
and ordering are trivial (and so persistence is just an INTEGER column).
Two factory methods cover the common construction paths:

* `TaxYear.from_label("2024/25")` for CLI / config input.
* `TaxYear.containing(some_date)`  for filtering a trade into its year.

Author: Emre Tezel
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date
from typing import Final

# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class InvalidTaxYearError(ValueError):
    """Raised when a tax-year input is malformed or out-of-range."""


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# UK tax year boundary: 6 April. Both sides of the inequality in `contains`
# have to agree on this, so declare it in one place.
_TAX_YEAR_START_MONTH: Final = 4
_TAX_YEAR_START_DAY: Final = 6

# We reject years before 2000 because (a) Interactive Brokers did not offer
# the products in our scope that long ago and (b) a tight sanity bound
# catches typos ("year 202" → 202 → silently accepted) that a more liberal
# bound would miss. This is a validation decision, not a hard CGT rule.
_MIN_START_YEAR: Final = 2000

# Canonical label shape, e.g. "2024/25". The two-digit suffix must match the
# final two digits of `start_year + 1` — we verify that separately after a
# regex match so the error message can be specific.
_LABEL_PATTERN: Final = re.compile(r"^(\d{4})/(\d{2})$")


# ---------------------------------------------------------------------------
# TaxYear
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True, order=True)
class TaxYear:
    """A single UK tax year identified by its starting calendar year.

    Attributes:
        start_year: The calendar year in which the tax year begins. For
            example, `TaxYear(2024)` represents 6 Apr 2024 → 5 Apr 2025,
            labelled "2024/25".
    """

    start_year: int

    def __post_init__(self) -> None:
        """Validate `start_year` is a plausible UK CGT year."""
        if not isinstance(self.start_year, int) or isinstance(self.start_year, bool):
            raise InvalidTaxYearError(
                f"start_year must be int, got {type(self.start_year).__name__}"
            )
        if self.start_year < _MIN_START_YEAR:
            raise InvalidTaxYearError(
                f"start_year must be >= {_MIN_START_YEAR}, got {self.start_year}"
            )

    # ------------------------------------------------------------------
    # Factories
    # ------------------------------------------------------------------

    @classmethod
    def from_label(cls, label: str) -> TaxYear:
        """Parse a canonical UK tax-year label like "2024/25".

        The two-digit suffix is not cosmetic: we verify it matches the
        year after `start_year`, so "2024/26" is rejected.
        """
        if not isinstance(label, str):
            raise InvalidTaxYearError(f"tax year label must be str, got {type(label).__name__}")
        match = _LABEL_PATTERN.fullmatch(label)
        if match is None:
            raise InvalidTaxYearError(f"tax year label must be 'YYYY/YY', got {label!r}")
        start_year = int(match.group(1))
        end_suffix = int(match.group(2))
        expected_suffix = (start_year + 1) % 100
        if end_suffix != expected_suffix:
            raise InvalidTaxYearError(
                f"tax year label {label!r} has inconsistent end suffix; "
                f"expected {expected_suffix:02d}, got {end_suffix:02d}"
            )
        return cls(start_year)

    @classmethod
    def containing(cls, d: date) -> TaxYear:
        """Return the tax year containing `d`.

        A date on or after 6 April falls in the tax year starting that
        calendar year; a date before 6 April falls in the previous one.
        """
        if not isinstance(d, date):
            raise TypeError(f"containing() expects a date, got {type(d).__name__}")
        boundary = date(d.year, _TAX_YEAR_START_MONTH, _TAX_YEAR_START_DAY)
        start_year = d.year if d >= boundary else d.year - 1
        return cls(start_year)

    # ------------------------------------------------------------------
    # Derived values
    # ------------------------------------------------------------------

    @property
    def start_date(self) -> date:
        """The inclusive first day of the tax year (6 April of `start_year`)."""
        return date(self.start_year, _TAX_YEAR_START_MONTH, _TAX_YEAR_START_DAY)

    @property
    def end_date(self) -> date:
        """The inclusive last day of the tax year (5 April of `start_year + 1`)."""
        return date(self.start_year + 1, _TAX_YEAR_START_MONTH, _TAX_YEAR_START_DAY - 1)

    @property
    def label(self) -> str:
        """The canonical label, e.g. "2024/25"."""
        return f"{self.start_year}/{(self.start_year + 1) % 100:02d}"

    # ------------------------------------------------------------------
    # Predicates and presentation
    # ------------------------------------------------------------------

    def contains(self, d: date) -> bool:
        """True iff `d` falls within this tax year (inclusive of both ends)."""
        return self.start_date <= d <= self.end_date

    def __repr__(self) -> str:
        """Compact identity for REPL / logs."""
        return f"TaxYear({self.label})"
