import pytest

from app.messaging.relay import backoff_delay


@pytest.mark.parametrize(
    ("attempts", "expected"), [(0, 1), (1, 2), (2, 4), (5, 32), (6, 60), (60, 60)]
)
def test_backoff_doubles_up_to_the_cap(attempts: int, expected: float) -> None:
    assert backoff_delay(attempts, base=1, cap=60) == expected


def test_any_number_of_attempts_is_fine() -> None:
    """2**1024 does not fit a float: without a bound on the exponent the relay would crash
    on a row that has been failing for about 17 hours."""
    assert backoff_delay(10_000, base=1, cap=60) == 60
