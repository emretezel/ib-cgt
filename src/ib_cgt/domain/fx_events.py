"""Provenance references for synthetic FX-pool event ids.

The FX rule engine receives non-trade cashflows — futures realisation
P&L, dividends / withholding tax, bond coupons — under caller-issued
*synthetic* integer ids so that `Acquisition.trade_id` /
`Disposal.trade_id` can stay plain integers across every source. Those
ids are internal plumbing: they are re-issued on every engine run and
mean nothing on their own. The types in this module are the durable
answer to "which real row produced this event?" — one small frozen
record per source kind, keyed by the ids that *are* stable across runs
(trade ids, dividend ids, coupon ids).

The calculator's runner builds a `Mapping[int, FXEventSource]` from
synthetic id to reference while it allocates the ids; the audit
renderers use it to print citeable labels (`P&L #A→#B`, `Div #N`,
`Cpn #N`), and the persisted-run tables use it so an FX matched
disposal keyed by a synthetic id can still be traced back to its
source after the run.

Author: Emre Tezel
"""

from __future__ import annotations

from dataclasses import dataclass


def _require_positive(name: str, value: int) -> None:
    """Reject non-positive ids — every referenced row has an id >= 1."""
    if value <= 0:
        raise ValueError(f"{name} must be a positive row id, got {value}")


@dataclass(frozen=True, slots=True, kw_only=True)
class FutureRealisationRef:
    """A futures realisation, identified by its open / close trade pair.

    A `FutureRealisation` has no row id of its own (it is re-derived
    from trades on every run), but the `(open_trade_id,
    close_trade_id)` pair is unique per run — the futures engine
    builds exactly one open slice per OPEN trade and drains it at
    most once per CLOSE trade — so the pair is the stable reference.
    """

    open_trade_id: int
    close_trade_id: int

    def __post_init__(self) -> None:
        """Both ids must be real (positive) trade ids and differ."""
        _require_positive("FutureRealisationRef.open_trade_id", self.open_trade_id)
        _require_positive("FutureRealisationRef.close_trade_id", self.close_trade_id)
        if self.open_trade_id == self.close_trade_id:
            raise ValueError("FutureRealisationRef: open and close trade ids must differ")


@dataclass(frozen=True, slots=True, kw_only=True)
class DividendRef:
    """A dividend / withholding-tax / payment-in-lieu row (`dividends.dividend_id`)."""

    dividend_id: int

    def __post_init__(self) -> None:
        """The dividend id must be a real (positive) row id."""
        _require_positive("DividendRef.dividend_id", self.dividend_id)


@dataclass(frozen=True, slots=True, kw_only=True)
class BondCouponRef:
    """A bond coupon payment row (`bond_coupons.bond_coupon_id`)."""

    bond_coupon_id: int

    def __post_init__(self) -> None:
        """The coupon id must be a real (positive) row id."""
        _require_positive("BondCouponRef.bond_coupon_id", self.bond_coupon_id)


# Sealed union of every source a synthetic FX event id can point at.
# Consumers branch with `isinstance`; adding a source means adding a
# member here and a branch everywhere mypy flags as non-exhaustive.
FXEventSource = FutureRealisationRef | DividendRef | BondCouponRef
