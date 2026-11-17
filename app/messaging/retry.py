from dataclasses import dataclass
from typing import Any

# Set by this service on every retry copy, not by RabbitMQ. The first delivery has none.
ATTEMPT_HEADER = "x-attempt"
LAST_ERROR_HEADER = "x-last-error"
# How many times delivery was postponed because the client's circuit was open.
POSTPONED_HEADER = "x-postponed"
# Set by RabbitMQ when it dead-letters a message. A republished copy is a new message, and
# since RabbitMQ 4.0 the broker ignores these headers coming from a publisher anyway.
_BROKER_HEADER_PREFIXES = ("x-death", "x-first-death-", "x-last-death-")


def own_headers(headers: dict[str, Any]) -> dict[str, Any]:
    """Headers of a delivered message without the ones the broker added."""
    return {k: v for k, v in headers.items() if not k.startswith(_BROKER_HEADER_PREFIXES)}


@dataclass(frozen=True)
class RetryPolicy:
    """Attempts are numbered from 1. After attempt n fails the message waits
    base_delay * 2**(n-1) and comes back; after max_attempts it goes to the DLQ.
    Defaults: 3 attempts, waits of 2 s and 4 s."""

    max_attempts: int
    base_delay_seconds: float

    def __post_init__(self) -> None:
        if self.max_attempts < 1 or self.base_delay_seconds <= 0:
            raise ValueError("max_attempts >= 1 and base_delay_seconds > 0 are required")

    def delay_after(self, attempt: int) -> float | None:
        """Wait before the next attempt, or None when this was the last one."""
        if attempt >= self.max_attempts:
            return None
        return self.base_delay_seconds * 2 ** (attempt - 1)

    @property
    def delays(self) -> list[float]:
        return [self.base_delay_seconds * 2**i for i in range(self.max_attempts - 1)]


def attempt_of(headers: dict[str, Any]) -> int:
    """Attempt number of a delivered message. Garbage in the header counts as the first try
    rather than crashing the consumer."""
    return max(_int_header(headers, ATTEMPT_HEADER, 1), 1)


def postponements_of(headers: dict[str, Any]) -> int:
    return max(_int_header(headers, POSTPONED_HEADER, 0), 0)


def _int_header(headers: dict[str, Any], name: str, default: int) -> int:
    try:
        return int(headers.get(name, default))
    except (TypeError, ValueError):
        return default


def delay_ms(seconds: float) -> int:
    return round(seconds * 1000)
