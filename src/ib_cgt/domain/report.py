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

from dataclasses import dataclass

from ib_cgt.domain.disposal import MatchedDisposal
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
            `matched_disposals`; the calculator computes these once so
            every renderer reads the same totals.
    """

    tax_year: TaxYear
    matched_disposals: tuple[MatchedDisposal, ...]
    summaries: tuple[AssetClassSummary, ...]

    def __post_init__(self) -> None:
        """Summaries must not double-up on the same asset class."""
        seen: set[AssetClass] = set()
        for summary in self.summaries:
            if summary.asset_class in seen:
                raise ValueError(
                    f"TaxYearReport.summaries contains duplicate asset class {summary.asset_class}"
                )
            seen.add(summary.asset_class)

    @property
    def net_gbp(self) -> Money:
        """Net gain (or loss) across every asset class in this report."""
        # Start from zero GBP so a report with no summaries still produces a
        # valid Money value rather than raising.
        total = Money.zero("GBP")
        for summary in self.summaries:
            total = total + summary.net_gbp
        return total

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
