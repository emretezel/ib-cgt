"""Tests for the FX-event provenance references in `ib_cgt.domain.fx_events`."""

from __future__ import annotations

import pytest

from ib_cgt.domain import BondCouponRef, DividendRef, FutureRealisationRef, FXEventSource


def test_refs_are_value_objects() -> None:
    a = FutureRealisationRef(open_trade_id=1, close_trade_id=2)
    b = FutureRealisationRef(open_trade_id=1, close_trade_id=2)
    assert a == b
    assert hash(a) == hash(b)
    assert len({a, b, DividendRef(dividend_id=1), BondCouponRef(bond_coupon_id=1)}) == 3


def test_realisation_ref_rejects_identical_ids() -> None:
    with pytest.raises(ValueError, match="must differ"):
        FutureRealisationRef(open_trade_id=7, close_trade_id=7)


@pytest.mark.parametrize(
    "build",
    [
        lambda: FutureRealisationRef(open_trade_id=0, close_trade_id=1),
        lambda: FutureRealisationRef(open_trade_id=1, close_trade_id=-3),
        lambda: DividendRef(dividend_id=0),
        lambda: BondCouponRef(bond_coupon_id=-1),
    ],
)
def test_refs_reject_non_positive_ids(build: object) -> None:
    assert callable(build)
    with pytest.raises(ValueError, match="positive row id"):
        build()


def test_union_members_are_distinguishable() -> None:
    sources: list[FXEventSource] = [
        FutureRealisationRef(open_trade_id=1, close_trade_id=2),
        DividendRef(dividend_id=3),
        BondCouponRef(bond_coupon_id=4),
    ]
    kinds = [type(s).__name__ for s in sources]
    assert kinds == ["FutureRealisationRef", "DividendRef", "BondCouponRef"]
