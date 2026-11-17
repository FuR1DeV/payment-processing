import pytest

from app.messaging.retry import ATTEMPT_HEADER, RetryPolicy, attempt_of, delay_ms


def test_three_attempts_wait_two_then_four_seconds() -> None:
    policy = RetryPolicy(max_attempts=3, base_delay_seconds=2)
    assert policy.delays == [2, 4]
    assert policy.delay_after(1) == 2
    assert policy.delay_after(2) == 4
    assert policy.delay_after(3) is None  # the third failure goes to the DLQ


def test_single_attempt_means_no_retries() -> None:
    policy = RetryPolicy(max_attempts=1, base_delay_seconds=2)
    assert policy.delays == []
    assert policy.delay_after(1) is None


def test_attempt_beyond_the_limit_is_not_retried() -> None:
    # E.g. the limit was lowered while copies with a higher counter were waiting.
    assert RetryPolicy(max_attempts=3, base_delay_seconds=1).delay_after(7) is None


@pytest.mark.parametrize(("max_attempts", "base"), [(0, 1.0), (3, 0.0), (3, -1.0)])
def test_invalid_policy_is_rejected(max_attempts: int, base: float) -> None:
    with pytest.raises(ValueError, match="required"):
        RetryPolicy(max_attempts=max_attempts, base_delay_seconds=base)


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        ({}, 1),
        ({ATTEMPT_HEADER: 2}, 2),
        ({ATTEMPT_HEADER: "3"}, 3),
        ({ATTEMPT_HEADER: "garbage"}, 1),
        ({ATTEMPT_HEADER: None}, 1),
        ({ATTEMPT_HEADER: 0}, 1),
        ({ATTEMPT_HEADER: -5}, 1),
    ],
)
def test_attempt_of_headers(headers: dict[str, object], expected: int) -> None:
    assert attempt_of(headers) == expected


def test_delay_ms() -> None:
    assert delay_ms(2) == 2000
    assert delay_ms(0.05) == 50
