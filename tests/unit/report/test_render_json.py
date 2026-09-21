"""Tests for `ib_cgt.report.render.to_json`.

Author: Emre Tezel
"""

from __future__ import annotations

import json

from ib_cgt.report import render_json, report_to_dict

from .conftest import sample_report


def test_json_round_trips_the_figures_at_full_precision() -> None:
    payload = json.loads(render_json(sample_report()))
    assert payload["tax_year"] == {
        "label": "2025/26",
        "start_date": "2025-04-06",
        "end_date": "2026-04-05",
    }
    assert payload["run"] == {"run_id": 7, "computed_at": "2026-09-21T15:00:00+00:00"}
    assert payload["complete"] is True
    listed, other = payload["sections"]
    assert listed["kind"] == "listed_shares"
    assert listed["boxes"] == {
        "disposals": 23,
        "proceeds": 24,
        "allowable_costs": 25,
        "gains": 26,
        "losses": 27,
    }
    assert listed["figures"] == {
        "disposal_count": 1,
        "proceeds_gbp": "3000",
        "allowable_costs_gbp": "2505",
        "gains_gbp": "495",
        "losses_gbp": "0",
        "net_gbp": "495",
    }
    assert [p["asset_class"] for p in other["by_asset_class"]] == ["future", "fx"]
    assert payload["totals"]["net_gbp"] == "4492"
    assert payload["issues"][0]["severity"] == "warning"
    assert payload["issues"][0]["instrument"]["symbol"] == "TSLA"


def test_json_disposals_carry_lines_and_tagged_bases() -> None:
    payload = report_to_dict(sample_report())
    stock, future, currency = payload["disposals"]
    assert stock["instrument"]["conid"] == 66468935
    assert stock["disposal_events"][0]["label"] == "#5"
    assert stock["gain_gbp"] == "495"
    first, second = stock["lines"]
    assert first["basis"] == {
        "kind": "direct",
        "match_rule": "same_day",
        "acquisition": {
            "event_id": 4,
            "label": "#4",
            "date": "2025-06-20",
            "account_id": "U1",
            "description": "stock AAPL buy 10 @ 90 USD",
        },
    }
    assert first["gross_proceeds_gbp"] == "1000"
    assert first["gain_gbp"] == "97"
    assert second["basis"]["kind"] == "pool"
    assert second["basis"]["average_cost_gbp"] == "80"
    assert future["lines"][0]["basis"]["kind"] == "close_out"
    assert future["lines"][0]["basis"]["gross_pnl_native"] == {"amount": "5000", "currency": "USD"}
    assert future["instrument"]["expiry_date"] == "2025-12-19"
    assert currency["instrument"]["currency_pair"] == {"base": "USD", "quote": "GBP"}
    assert currency["lines"][0]["disposal"]["label"] == "Cash #3"


def test_summary_only_json_omits_the_disposals() -> None:
    payload = json.loads(render_json(sample_report(), include_disposals=False))
    assert "disposals" not in payload
    assert payload["sections"][0]["figures"]["disposal_count"] == 1
