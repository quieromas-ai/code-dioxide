from datetime import UTC, datetime

import pytest

from src.reports.query import (
    DEFAULT_WINDOW_S,
    Filters,
    QueryError,
    bucket_start,
    decode_cursor,
    encode_cursor,
    parse_bucket,
    parse_filters,
    parse_group_by,
    parse_limit,
    parse_time,
    where,
)

NOW = 1000.0 + DEFAULT_WINDOW_S


def test_parse_time_epoch_and_iso() -> None:
    assert parse_time("1795221434") == 1795221434.0
    assert parse_time("1795221434.5") == 1795221434.5
    expected = datetime(2026, 11, 26, 10, tzinfo=UTC).timestamp()
    assert parse_time("2026-11-26T10:00:00Z") == expected
    assert parse_time("2026-11-26T10:00:00") == expected
    assert parse_time("2026-11-26T11:00:00+01:00") == expected
    for bad in ("x", "nan", "inf"):
        with pytest.raises(QueryError):
            parse_time(bad)


def test_parse_filters_window() -> None:
    assert parse_filters({}, now=NOW, windowed=True) == Filters(since=1000.0, until=NOW)
    assert parse_filters({}, now=NOW, windowed=False) == Filters()
    got = parse_filters(
        {"agent": "a", "task": "t", "project": "/p", "status": "running", "kind": "single",
         "since": "5", "until": "9"},
        now=NOW, windowed=True,
    )  # fmt: skip
    assert got == Filters("a", "t", "/p", "running", "single", 5.0, 9.0)


@pytest.mark.parametrize(
    "params",
    [{"since": "9", "until": "5"}, {"kind": "x"}, {"status": "done"}, {"stage_agent": "a"}],
)
def test_parse_filters_rejects(params: dict[str, str]) -> None:
    with pytest.raises(QueryError):
        parse_filters(params, now=NOW, windowed=True)


def test_stage_agent_parsed_where_supported() -> None:
    found = parse_filters({"stage_agent": "dx-qa"}, now=NOW, windowed=False, stage_agent=True)
    assert found.stage_agent == "dx-qa"
    assert parse_filters({}, now=NOW, windowed=False, stage_agent=True).stage_agent is None


def test_stage_agent_message() -> None:
    with pytest.raises(QueryError, match="stage_agent"):
        parse_filters({"stage_agent": "a"}, now=NOW, windowed=False)


def test_parse_group_by() -> None:
    allowed = ("agent", "project", "kind")
    assert parse_group_by(None, allowed) == ()
    assert parse_group_by("", allowed) == ()
    assert parse_group_by("agent,project", allowed) == ("agent", "project")
    with pytest.raises(QueryError, match="agent, project, kind"):
        parse_group_by("model", allowed)


def test_bucket() -> None:
    assert parse_bucket(None) == "none"
    assert parse_bucket("day") == "day"
    with pytest.raises(QueryError):
        parse_bucket("week")
    assert bucket_start(1795221434.0, "hour") == 1795219200.0
    assert bucket_start(1795221434.0, "day") == 1795219200.0
    assert bucket_start(1795305599.0, "day") == 1795219200.0
    assert bucket_start(1795221434.0, "none") is None


def test_parse_limit() -> None:
    assert parse_limit(None) == 100
    assert parse_limit("5000") == 1000
    assert parse_limit("0") == 1
    with pytest.raises(QueryError):
        parse_limit("a")


def test_cursor_round_trip() -> None:
    assert decode_cursor(encode_cursor(["/p", 3])) == ["/p", 3]
    with pytest.raises(QueryError):
        decode_cursor("!!")
    with pytest.raises(QueryError):
        decode_cursor(encode_cursor({"a": 1}))  # type: ignore[arg-type]


def test_where_fragment() -> None:
    sql, params = where(Filters(agent="a", kind="single", since=1.0, until=2.0), time_column="x.at")
    assert sql == "t.agent = ? AND t.kind = ? AND x.at >= ? AND x.at < ?"
    assert params == ["a", "single", 1.0, 2.0]
    assert where(Filters(), time_column=None) == ("1", [])
