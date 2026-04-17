"""
Unit tests for `filter_on_rrp` — offline, fixture-driven.

Motivation
    The pre-existing rrp.py filter had a boolean-precedence bug that
    reduced to `operationType == "REVERSE REPO"`, silently including
    Term RRP and FIMA RRP. This regression test locks down the correct
    behaviour using synthetic fixtures modelled after the NY Fed
    all/results/lastTwoWeeks.json schema.
"""
from __future__ import annotations

from ..collectors.rrp import filter_on_rrp


# Schema mirrors the real NY Fed response; only fields the filter reads.
_OVERNIGHT = {
    "operationId": "RP 041626 27",
    "operationDate": "2026-04-16",
    "operationType": "Reverse Repo",
    "term": "Overnight",
    "termCalenderDays": 1,
    "totalAmtAccepted": 158_000_000,
}

_OVERNIGHT_WEEKEND = {
    # Friday operation settling over weekend: term=Overnight, termCalenderDays=3
    "operationId": "RP 041026 27",
    "operationDate": "2026-04-10",
    "operationType": "Reverse Repo",
    "term": "Overnight",
    "termCalenderDays": 3,
    "totalAmtAccepted": 507_000_000,
}

_TERM_RRP = {
    # Multi-day Term RRP — must be excluded
    "operationId": "RP 031826 30",
    "operationDate": "2026-03-18",
    "operationType": "Reverse Repo",
    "term": "Term",
    "termCalenderDays": 7,
    "totalAmtAccepted": 5_000_000_000,
}

_FIMA = {
    # FIMA Repo Facility — different operationType, must be excluded
    "operationId": "FIMA 041626 01",
    "operationDate": "2026-04-16",
    "operationType": "FIMA Repo",
    "term": "Overnight",
    "totalAmtAccepted": 2_000_000_000,
}

_REPO = {
    # SRF Repo (not Reverse Repo) — must be excluded
    "operationId": "RP 041526 25",
    "operationDate": "2026-04-15",
    "operationType": "Repo",
    "term": "Overnight",
    "totalAmtAccepted": 10_462_000_000,
}

_OLD_SCHEMA_NO_TERM = {
    # propositions/search.json returns no `term` field. Defensive policy:
    # treat as Overnight (true for all observed Reverse Repo ops in
    # 2021-2026 since Fed has not conducted Term RRP in this regime).
    "operationId": "RP 120122 27",
    "operationDate": "2022-12-01",
    "operationType": "Reverse Repo",
    "totalAmtAccepted": 2_000_000_000_000,
}


def test_keeps_overnight_weekday():
    out = filter_on_rrp([_OVERNIGHT])
    assert len(out) == 1
    assert out[0]["operationId"] == "RP 041626 27"


def test_keeps_overnight_weekend_three_day():
    # Weekend settlement — termCalenderDays=3 but term="Overnight"
    out = filter_on_rrp([_OVERNIGHT_WEEKEND])
    assert len(out) == 1


def test_excludes_term_rrp():
    out = filter_on_rrp([_TERM_RRP])
    assert out == []


def test_excludes_fima():
    out = filter_on_rrp([_FIMA])
    assert out == []


def test_excludes_repo():
    out = filter_on_rrp([_REPO])
    assert out == []


def test_defensive_missing_term_treated_as_overnight():
    """Old propositions/search.json lacks `term`. filter_on_rrp should
    keep the op to preserve historical backfill behaviour (documented
    safe because Fed hasn't done Term RRP in 2021-2026)."""
    out = filter_on_rrp([_OLD_SCHEMA_NO_TERM])
    assert len(out) == 1
    assert out[0]["operationId"] == "RP 120122 27"


def test_mixed_batch():
    """Realistic 2-week batch: mostly Overnight, with one Term and one
    Repo mixed in — classic case the old buggy filter mishandled."""
    ops = [_OVERNIGHT, _OVERNIGHT_WEEKEND, _TERM_RRP, _FIMA, _REPO, _OLD_SCHEMA_NO_TERM]
    out = filter_on_rrp(ops)
    ids = {o["operationId"] for o in out}
    assert ids == {"RP 041626 27", "RP 041026 27", "RP 120122 27"}


def test_case_insensitive_operation_type():
    op = {**_OVERNIGHT, "operationType": "reverse repo"}
    out = filter_on_rrp([op])
    assert len(out) == 1


def test_whitespace_tolerant():
    op = {**_OVERNIGHT, "operationType": "  Reverse Repo  ", "term": " Overnight "}
    out = filter_on_rrp([op])
    assert len(out) == 1


def test_empty_input():
    assert filter_on_rrp([]) == []
