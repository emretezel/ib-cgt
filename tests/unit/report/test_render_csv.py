"""Tests for `ib_cgt.report.render.to_csv`.

Author: Emre Tezel
"""

from __future__ import annotations

import csv
import io

from ib_cgt.report import CSV_HEADER, render_csv

from .conftest import sample_report


def test_csv_has_one_row_per_line_with_plain_numbers() -> None:
    report = sample_report()
    rows = list(csv.reader(io.StringIO(render_csv(report))))
    assert tuple(rows[0]) == CSV_HEADER
    assert len(rows) - 1 == sum(len(d.lines) for d in report.disposals) == 4
    by_name = [dict(zip(CSV_HEADER, row, strict=True)) for row in rows[1:]]

    same_day = by_name[0]
    assert same_day["section"] == "listed_shares"
    assert same_day["symbol"] == "AAPL"
    assert same_day["disposal_ref"] == "#5"
    assert same_day["disposal_account"] == "U2"
    assert same_day["rule"] == "same-day (s.105(1)(b))"
    assert same_day["acquisition_ref"] == "#4"
    assert same_day["acquisition_date"] == "2025-06-20"
    assert same_day["gross_proceeds_gbp"] == "1000"
    assert same_day["disposal_costs_gbp"] == "1"
    assert same_day["net_proceeds_gbp"] == "999"
    assert same_day["cost_gbp"] == "900"
    assert same_day["acquisition_costs_gbp"] == "2"
    assert same_day["allowable_costs_gbp"] == "902"
    assert same_day["gain_gbp"] == "97"

    pool = by_name[1]
    assert pool["acquisition_ref"] == "S.104 holding"
    assert pool["acquisition_date"] == ""
    assert pool["acquisition_description"].startswith("S.104 holding of 50 units, cost 4000 GBP")

    future = by_name[2]
    assert future["rule"] == "close-out (s.143)"
    assert future["acquisition_ref"] == "#8"
    assert "gross P&L 5000 USD" in future["acquisition_description"]
    assert future["gain_gbp"] == "3996"

    currency = by_name[3]
    assert currency["asset_class"] == "fx"
    assert currency["disposal_ref"] == "Cash #3"
    assert "," not in currency["gross_proceeds_gbp"]
