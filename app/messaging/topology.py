"""Exchanges and queues. Both the relay and the consumer declare them on start; declaring
is idempotent as long as the arguments match.

    payments (direct) --"payment.created"--> payments.new  <- consumer reads here
        failed, attempts left: a copy goes to
    payments.retry (direct) --"payments.new.retry.2000ms"--> payments.new.retry.2000ms
        (TTL 2 s, nobody reads it; on expiry the broker dead-letters the copy back to
        payments.new through the default exchange)
        failed for the last time: the consumer rejects it, and the broker moves it to
    payments.dlx (direct) --"payments.new"--> payments.new.dlq  <- for people, not code
"""

from dataclasses import dataclass

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.core.config import Settings
from app.domain.events import PAYMENT_CREATED

from .retry import RetryPolicy, delay_ms

# The default exchange ("") routes a message to the queue named by its routing key.
DEFAULT_EXCHANGE = ""


@dataclass(frozen=True)
class Topology:
    namespace: str
    retry_delays_ms: tuple[int, ...]
    # Where a delivery waits while the client's circuit is open (no attempt is spent).
    postpone_delay_ms: int

    @classmethod
    def from_settings(cls, settings: Settings) -> "Topology":
        policy = retry_policy(settings)
        return cls(
            namespace=settings.rabbitmq_namespace,
            retry_delays_ms=tuple(delay_ms(d) for d in policy.delays),
            postpone_delay_ms=delay_ms(settings.webhook_breaker_open_seconds),
        )

    @property
    def delays_ms(self) -> list[int]:
        """Every delay queue: the retry ones and the postponement one (may coincide)."""
        return sorted({*self.retry_delays_ms, self.postpone_delay_ms})

    # Events by type. The relay publishes every outbox event here, routing key = event type.
    @property
    def exchange(self) -> RabbitExchange:
        return RabbitExchange(self.namespace, type=ExchangeType.DIRECT, durable=True)

    @property
    def new_payments_queue_name(self) -> str:
        return f"{self.namespace}.new"

    @property
    def new_payments(self) -> RabbitQueue:
        return RabbitQueue(
            self.new_payments_queue_name,
            durable=True,
            routing_key=PAYMENT_CREATED,
            arguments={
                # reject(requeue=False) sends the message here instead of dropping it.
                "x-dead-letter-exchange": self.dead_letter_exchange.name,
                "x-dead-letter-routing-key": self.new_payments_queue_name,
            },
        )

    @property
    def retry_exchange(self) -> RabbitExchange:
        return RabbitExchange(f"{self.namespace}.retry", type=ExchangeType.DIRECT, durable=True)

    def retry_queue_name(self, delay: int) -> str:
        # The delay is part of the name: changing it later creates a new queue instead of
        # failing to redeclare the old one with another x-message-ttl.
        return f"{self.new_payments_queue_name}.retry.{delay}ms"

    def retry_queue(self, delay: int) -> RabbitQueue:
        return RabbitQueue(
            self.retry_queue_name(delay),
            durable=True,
            routing_key=self.retry_queue_name(delay),
            arguments={
                # Nobody consumes from a delay queue: messages just expire after the TTL.
                # One TTL per queue, so the head of the queue always expires first.
                "x-message-ttl": delay,
                # On expiry the broker dead-letters the copy straight back to payments.new
                # (and only there: not to other queues bound to "payment.created").
                "x-dead-letter-exchange": DEFAULT_EXCHANGE,
                "x-dead-letter-routing-key": self.new_payments_queue_name,
            },
        )

    @property
    def dead_letter_exchange(self) -> RabbitExchange:
        return RabbitExchange(f"{self.namespace}.dlx", type=ExchangeType.DIRECT, durable=True)

    @property
    def dead_letter_queue(self) -> RabbitQueue:
        return RabbitQueue(
            f"{self.new_payments_queue_name}.dlq",
            durable=True,
            routing_key=self.new_payments_queue_name,
        )


def retry_policy(settings: Settings) -> RetryPolicy:
    return RetryPolicy(
        max_attempts=settings.max_attempts, base_delay_seconds=settings.retry_base_delay_seconds
    )


async def declare(broker: RabbitBroker, topology: Topology) -> None:
    """Create (or check) every exchange, queue and binding. Safe to call again."""
    pairs = [
        (topology.exchange, topology.new_payments),
        (topology.dead_letter_exchange, topology.dead_letter_queue),
        *((topology.retry_exchange, topology.retry_queue(d)) for d in topology.delays_ms),
    ]
    for exchange_spec, queue_spec in pairs:
        exchange = await broker.declare_exchange(exchange_spec)
        queue = await broker.declare_queue(queue_spec)
        await queue.bind(exchange, routing_key=queue_spec.routing())
