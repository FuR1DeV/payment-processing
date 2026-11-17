import logging

from faststream.rabbit import Channel, RabbitBroker

from app.core.config import Settings
from app.db.models import OutboxEvent

from .topology import Topology


def create_broker(settings: Settings, *, prefetch: int | None = None) -> RabbitBroker:
    """aio-pika keeps a robust connection: after a RabbitMQ restart it reconnects by itself.

    Publisher confirms are on by default: publish() returns only after the broker has
    taken responsibility for the message (for a persistent message in a durable queue:
    written to disk). on_return_raises: a mandatory message that no queue accepts makes
    publish() raise instead of quietly returning.
    """
    return RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(prefetch_count=prefetch, on_return_raises=True),
        # In-flight handlers (the gateway takes up to 5 s) get this long to finish on stop.
        graceful_timeout=30,
        logger=logging.getLogger("payments.broker"),
    )


class RabbitEventPublisher:
    """Publishes outbox events to the events exchange; the routing key is the event type."""

    def __init__(self, broker: RabbitBroker, topology: Topology, timeout: float = 10.0) -> None:
        self._broker = broker
        self._topology = topology
        self._timeout = timeout

    async def publish(self, event: OutboxEvent) -> None:
        await self._broker.publish(
            event.payload,
            exchange=self._topology.exchange,
            routing_key=event.event_type,
            # The consumer and the logs can tell a repeated delivery by message_id.
            message_id=str(event.event_id),
            content_type="application/json",
            # delivery_mode=2: written to disk, survives a broker restart. Off by default.
            persist=True,
            # No queue bound for this key: the broker returns the message and publish()
            # raises, so the outbox row stays unpublished instead of the event vanishing.
            mandatory=True,
            # Without a timeout publish() waits for the confirm forever.
            timeout=self._timeout,
        )
