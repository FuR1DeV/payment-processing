"""Dead letters: look at them and, once the cause is fixed, send them back to work.

    python -m app.messaging.dlq            how many messages wait in the DLQ
    python -m app.messaging.dlq --replay   move them back to payments.new, attempts from 1

The message bodies can be read in the RabbitMQ UI (queue payments.new.dlq, Get messages).
Replaying is safe: processing is idempotent, a settled payment only gets its webhook.
"""

import argparse
import asyncio
import logging

from faststream.rabbit import RabbitBroker

from app.core.config import get_settings
from app.core.logging import configure_logging

from .broker import create_broker
from .retry import ATTEMPT_HEADER, LAST_ERROR_HEADER, POSTPONED_HEADER, own_headers
from .topology import Topology, declare

logger = logging.getLogger("payments.dlq")


async def dead_letter_count(broker: RabbitBroker, topology: Topology) -> int:
    queue = await broker.declare_queue(topology.dead_letter_queue)  # cached by FastStream
    result = await queue.declare()  # a fresh declare returns the current message count
    return result.message_count or 0


async def replay_dead_letters(broker: RabbitBroker, topology: Topology) -> int:
    """Move the dead letters that are there now back to payments.new. Returns how many.

    Only as many as the queue held at the start: a message that fails again returns to the
    DLQ a few seconds later, and draining "until empty" could go round in circles.
    """
    dlq = await broker.declare_queue(topology.dead_letter_queue)
    to_move = await dead_letter_count(broker, topology)
    moved = 0
    while moved < to_move and (message := await dlq.get(no_ack=False, fail=False)) is not None:
        headers = own_headers(dict(message.headers))
        for name in (ATTEMPT_HEADER, LAST_ERROR_HEADER, POSTPONED_HEADER):
            headers.pop(name, None)
        try:
            await broker.publish(
                message.body,
                # Default exchange: straight into payments.new, not to every queue bound to
                # "payment.created".
                exchange="",
                routing_key=topology.new_payments_queue_name,
                headers=headers,
                message_id=message.message_id,
                content_type=message.content_type,
                persist=True,
                mandatory=True,
                timeout=10,  # without it publish() may wait for the confirm for ever
            )
        except Exception:
            await message.nack(requeue=True)
            raise
        # Removed from the DLQ only once the copy is confirmed.
        await message.ack()
        moved += 1
    return moved


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--replay", action="store_true", help="move dead letters back")
    args = parser.parse_args()

    settings = get_settings()
    configure_logging(settings.log_level)
    topology = Topology.from_settings(settings)
    broker = create_broker(settings)
    await broker.connect()
    try:
        await declare(broker, topology)
        if args.replay:
            print(f"moved back to {topology.new_payments_queue_name}:", end=" ")
            print(await replay_dead_letters(broker, topology))
        else:
            print(f"{topology.dead_letter_queue.name}:", await dead_letter_count(broker, topology))
    finally:
        await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
