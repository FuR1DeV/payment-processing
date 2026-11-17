"""The consumer of payments.new: one handler that processes the payment, notifies the client
and decides what happens to the message.

Run: faststream run app.messaging.consumer:create_app --factory
"""

import logging
from collections.abc import Awaitable, Callable
from dataclasses import asdict
from typing import Any
from uuid import UUID

from faststream import AckPolicy, FastStream
from faststream.rabbit import RabbitBroker, RabbitMessage
from faststream.rabbit.message import RabbitMessage as StreamMessage
from pydantic import ValidationError

from app.bootstrap import build_processor
from app.core.config import get_settings
from app.core.logging import configure_logging, log_context
from app.domain.events import PaymentCreated
from app.services.ports import WebhookPostponedError
from app.services.processing import PaymentNotFoundError, ProcessingReport

from .broker import create_broker
from .retry import (
    ATTEMPT_HEADER,
    LAST_ERROR_HEADER,
    POSTPONED_HEADER,
    RetryPolicy,
    attempt_of,
    delay_ms,
    own_headers,
    postponements_of,
)
from .topology import Topology, declare, retry_policy

logger = logging.getLogger("payments.consumer")

RETRY_PUBLISH_TIMEOUT_SECONDS = 10.0


class PaymentEventHandler:
    """Outcome of one delivery:

    - processed (or already processed earlier): ack;
    - failed and attempts are left: publish a copy with attempt + 1 to the delay queue,
      then ack the original;
    - failed on the last attempt: reject(requeue=False), RabbitMQ moves it to the DLQ;
    - the client's circuit is open (its server is known to be down): the webhook was not
      even tried, so a copy with the same attempt number waits in the postponement queue;
      after too many postponements the message goes to the DLQ;
    - cannot ever succeed (broken body, unknown payment): reject at once, no retries;
    - the copy cannot be published: reject into the DLQ rather than spin.
    """

    def __init__(
        self,
        *,
        process_payment: Callable[[UUID], Awaitable[ProcessingReport]],
        broker: RabbitBroker,
        topology: Topology,
        policy: RetryPolicy,
        max_postponements: int,
    ) -> None:
        self._process = process_payment
        self._broker = broker
        self._topology = topology
        self._policy = policy
        self._max_postponements = max_postponements

    async def handle(self, message: RabbitMessage) -> None:
        attempt = attempt_of(message.headers)
        with log_context(message_id=message.message_id, attempt=attempt):
            try:
                event = PaymentCreated.model_validate_json(message.body)
            except ValidationError:
                logger.error("message_malformed_to_dlq")
                await message.reject(requeue=False)
                return

            with log_context(payment_id=str(event.payment_id)):
                try:
                    report = await self._process(event.payment_id)
                except PaymentNotFoundError:
                    logger.error("payment_not_found_to_dlq")
                    await message.reject(requeue=False)
                except WebhookPostponedError as exc:
                    await self._postpone(message, attempt, exc)
                except Exception as exc:
                    await self._retry_or_dead_letter(message, attempt, exc)
                else:
                    logger.info("payment_handled", extra=asdict(report))
                    await message.ack()

    async def _retry_or_dead_letter(
        self, message: RabbitMessage, attempt: int, exc: Exception
    ) -> None:
        error = f"{type(exc).__name__}: {exc}"
        delay = self._policy.delay_after(attempt)
        if delay is None:
            logger.error("attempts_exhausted_to_dlq", extra={"error": error})
            await message.reject(requeue=False)
            return
        logger.warning("processing_failed_will_retry", extra={"error": error, "delay": delay})
        headers = own_headers(message.headers) | {
            ATTEMPT_HEADER: attempt + 1,
            LAST_ERROR_HEADER: error[:250],
        }
        await self._send_copy(message, delay_ms(delay), headers)

    async def _postpone(
        self, message: RabbitMessage, attempt: int, exc: WebhookPostponedError
    ) -> None:
        # Not an attempt: nothing was sent, so the attempt counter stays. Postponements are
        # counted separately and are limited too: while the host stays down and new
        # payments keep coming, only one probe per open period spends an attempt, so
        # without a limit the messages would circle through the delay queue for days.
        postponed = postponements_of(message.headers) + 1
        if postponed > self._max_postponements:
            logger.error("postponed_too_long_to_dlq", extra={"host": exc.host})
            await message.reject(requeue=False)
            return
        logger.info(
            "webhook_postponed_circuit_open",
            extra={"host": exc.host, "postponed": postponed},
        )
        headers = own_headers(message.headers) | {
            ATTEMPT_HEADER: attempt,
            POSTPONED_HEADER: postponed,
        }
        await self._send_copy(message, self._topology.postpone_delay_ms, headers)

    async def _send_copy(self, message: RabbitMessage, delay: int, headers: dict[str, Any]) -> None:
        try:
            await self._broker.publish(
                message.body,
                exchange=self._topology.retry_exchange,
                routing_key=self._topology.retry_queue_name(delay),
                headers=headers,
                message_id=message.message_id,
                correlation_id=message.correlation_id,
                content_type=message.content_type,
                persist=True,
                mandatory=True,
                timeout=RETRY_PUBLISH_TIMEOUT_SECONDS,
            )
        except Exception:
            # The copy is not confirmed. Requeueing would spin the message at full speed for
            # as long as the publishing keeps failing, so it goes to the DLQ: kept, visible,
            # and replayable (a pending payment is also picked up by the stale sweep).
            logger.exception("copy_publish_failed_to_dlq")
            await message.reject(requeue=False)
            return
        # Ack only after the broker confirmed the copy. A crash in between leaves both: a
        # duplicate the idempotent processing absorbs, never a lost message.
        await message.ack()


def _raw_body(message: StreamMessage) -> bytes:
    """Give the handler the body as is. FastStream's own decoder parses JSON before the
    handler runs, and a broken body would fail where the handler cannot see it."""
    return message.body


def subscribe(broker: RabbitBroker, topology: Topology, handler: PaymentEventHandler) -> None:
    broker.subscriber(
        topology.new_payments,
        topology.exchange,
        decoder=_raw_body,
        # The handler settles every message itself (ack, reject or nack). The policy is a
        # safety net: if something unexpected escapes it, the message is rejected into the
        # DLQ instead of hanging unacknowledged. After an explicit ack it does nothing.
        ack_policy=AckPolicy.REJECT_ON_ERROR,
    )(handler.handle)


def create_app() -> FastStream:
    settings = get_settings()
    configure_logging(settings.log_level)
    # prefetch: at most this many unacknowledged messages are handed to this consumer.
    broker = create_broker(settings, prefetch=settings.consumer_prefetch)
    topology = Topology.from_settings(settings)
    processor = build_processor(settings)
    handler = PaymentEventHandler(
        process_payment=processor.process_payment,
        broker=broker,
        topology=topology,
        policy=retry_policy(settings),
        max_postponements=settings.webhook_max_postponements,
    )
    subscribe(broker, topology, handler)
    app = FastStream(broker)

    @app.on_startup
    async def declare_topology() -> None:
        # on_startup runs before the subscriber starts consuming: the delay queues and the
        # DLQ exist before the first failure needs them (a dead letter sent to a missing
        # exchange is silently dropped by RabbitMQ).
        await broker.connect()
        await declare(broker, topology)

    @app.after_shutdown
    async def close() -> None:
        await processor.close()

    return app
