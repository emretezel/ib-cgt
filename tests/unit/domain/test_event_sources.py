"""Tests for the event provenance references in `ib_cgt.domain.event_sources`."""

from __future__ import annotations

import pytest

from ib_cgt.domain import (
    BondCouponRef,
    CashEventRef,
    CorporateActionRef,
    DividendRef,
    EventSource,
    FutureRealisationRef,
)


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
        lambda: CashEventRef(cash_event_id=0),
        lambda: CorporateActionRef(corporate_action_id=0),
    ],
)
def test_refs_reject_non_positive_ids(build: object) -> None:
    assert callable(build)
    with pytest.raises(ValueError, match="positive row id"):
        build()


def test_union_members_are_distinguishable() -> None:
    sources: list[EventSource] = [
        FutureRealisationRef(open_trade_id=1, close_trade_id=2),
        DividendRef(dividend_id=3),
        BondCouponRef(bond_coupon_id=4),
        CashEventRef(cash_event_id=5),
        CorporateActionRef(corporate_action_id=6),
    ]
    kinds = [type(s).__name__ for s in sources]
    assert kinds == [
        "FutureRealisationRef",
        "DividendRef",
        "BondCouponRef",
        "CashEventRef",
        "CorporateActionRef",
    ]


def test_corporate_action_ref_is_the_fifth_union_member() -> None:
    ref = CorporateActionRef(corporate_action_id=5)
    assert ref == CorporateActionRef(corporate_action_id=5)
    # Typed through the union so the inequality is a real runtime check
    # rather than one mypy can prove non-overlapping statically.
    other: EventSource = CashEventRef(cash_event_id=5)
    assert ref != other
