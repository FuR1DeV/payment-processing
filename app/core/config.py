from functools import lru_cache

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

MIN_SECRET_LENGTH = 16


class Settings(BaseSettings):
    """Settings come from the environment (or .env). Secrets have no defaults."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    log_level: str = "INFO"
    # Swagger UI and the OpenAPI schema: on for local runs, off anywhere real.
    docs_enabled: bool = True
    # Demo page at /demo/ with its helper endpoints /demo/api/*: on for local runs, off
    # anywhere real. The helpers read the outbox, the queues and the demo webhook receiver.
    demo_page_enabled: bool = True
    demo_receiver_url: str = "http://webhook-receiver:9000"

    # Static key for the X-API-Key header. Without it the service does not start.
    api_key: SecretStr
    # HMAC secret for outgoing webhooks; the client verifies the signature with it.
    webhook_secret: SecretStr

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    database_pool_size: int = 10

    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    # Prefix of every exchange and queue name. Tests use their own prefix to stay isolated.
    rabbitmq_namespace: str = "payments"

    # Outbox relay.
    outbox_poll_interval_seconds: float = 0.5
    outbox_batch_size: int = 100
    outbox_retry_base_seconds: float = 1.0
    outbox_retry_max_seconds: float = 60.0
    # Safety net: a payment still pending this long after its last event gets a new event
    # (its message went to the DLQ during a database blip, or was lost by the broker).
    # Each next fresh event waits twice as long (5, 10, 20 min) and there are at most
    # this many: a payment stuck for good is not fed to the DLQ for ever.
    stale_pending_after_seconds: float = 300.0
    stale_pending_max_requeues: int = Field(default=3, ge=0)
    stale_check_interval_seconds: float = 30.0

    # Consumer: attempts in total (the first one included) and the delay before the second one.
    # Each next delay is twice as long: 2 s, 4 s with the defaults.
    max_attempts: int = Field(default=3, ge=1, le=10)
    retry_base_delay_seconds: float = Field(default=2.0, gt=0)
    # How many messages the consumer handles at the same time (RabbitMQ prefetch).
    consumer_prefetch: int = Field(default=10, ge=1)

    # Payment gateway emulator.
    gateway_min_delay_seconds: float = 2.0
    gateway_max_delay_seconds: float = 5.0
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)

    # Outgoing webhooks.
    webhook_timeout_seconds: float = 5.0
    # Circuit breaker per client host: this many "server is down" failures in a row open
    # it; while open, deliveries to the host are postponed by open_seconds without a request
    # and without spending an attempt.
    webhook_breaker_failures: int = Field(default=5, ge=1)
    webhook_breaker_open_seconds: float = Field(default=30.0, gt=0)
    # Then the message goes to the DLQ: 120 postponements of 30 s is an hour.
    webhook_max_postponements: int = Field(default=120, ge=0)

    @field_validator("api_key", "webhook_secret")
    @classmethod
    def _long_enough(cls, value: SecretStr) -> SecretStr:
        if len(value.get_secret_value()) < MIN_SECRET_LENGTH:
            raise ValueError(f"must be at least {MIN_SECRET_LENGTH} characters long")
        return value


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]  # required values come from the environment
