import pytest

from src.stages import StageNumbers, normalize_stage, stage_numbers

ALL_NONE = StageNumbers(None, None, None, None, None, None, None)


@pytest.mark.parametrize(
    ("label", "stage"),
    [
        ("review-r1-dx-reviewer-correctness@a1", "review-dx-reviewer-correctness"),
        ("qa-r1@a1", "qa"),
        ("implement@a2", "implement"),
        ("plan", "plan"),
        ("interpret-plan-1", "interpret-plan-1"),
        ("fix-r12@a3", "fix"),
    ],
)
def test_normalize_stage(label: str, stage: str) -> None:
    assert normalize_stage(label) == stage


def test_stage_numbers_from_progress_entry() -> None:
    entry = {
        "agentType": "dx-qa", "model": "m", "tokens": 55092, "toolCalls": 14,
        "lastToolName": "Bash", "durationMs": 132148, "lastProgressAt": 1795366215414,
    }  # fmt: skip
    assert stage_numbers(entry) == StageNumbers(
        "dx-qa", "m", 55092, 14, "Bash", 132.148, 1795366215.414
    )


def test_stage_numbers_of_cached_or_missing_entry_are_none() -> None:
    assert stage_numbers({"cached": True, "tokens": None, "agentType": "x"}) == ALL_NONE
    assert stage_numbers(None) == ALL_NONE


def test_stage_numbers_reject_non_numeric_and_negative() -> None:
    found = stage_numbers({"tokens": "x", "durationMs": -1, "toolCalls": True})
    assert (found.stage_tokens, found.duration_s, found.tool_calls) == (None, None, None)
