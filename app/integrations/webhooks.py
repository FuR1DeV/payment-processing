"""Outgoing webhooks: HTTP delivery and the HMAC signature the client verifies."""

import asyncio
import hashlib
import hmac
from datetime import datetime

import httpx
from pydantic import SecretStr

from app.core.clock import Clock, utc_now
from app.domain.events import PaymentWebhook
from app.services.ports import WebhookDeliveryError

ID_HEADER = "X-Webhook-Id"
TIMESTAMP_HEADER = "X-Webhook-Timestamp"
SIGNATURE_HEADER = "X-Webhook-Signature"
# A signature older than this is rejected by the receiver (protects against replaying).
SIGNATURE_TOLERANCE_SECONDS = 300


def sign(secret: bytes, timestamp: str, body: bytes) -> str:
    """sha256=HMAC(secret, "<timestamp>.<raw body>"), the scheme Stripe and others use."""
    digest = hmac.new(secret, timestamp.encode() + b"." + body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


def verify(secret: bytes, timestamp: str, body: bytes, signature: str, *, now: datetime) -> bool:
    try:
        sent_at = int(timestamp)
    except ValueError:
        return False
    if abs(now.timestamp() - sent_at) > SIGNATURE_TOLERANCE_SECONDS:
        return False
    # Bytes, not str: compare_digest raises on non-ASCII strings instead of returning False.
    expected = sign(secret, timestamp, body).encode()
    return hmac.compare_digest(expected, signature.encode("utf-8", "replace"))


class HttpWebhookSender:
    def __init__(
        self,
        client: httpx.AsyncClient,
        secret: SecretStr,
        *,
        timeout_seconds: float = 5.0,
        clock: Clock = utc_now,
    ) -> None:
        self._client = client
        self._secret = secret.get_secret_value().encode()
        self._timeout = timeout_seconds
        self._clock = clock

    async def send(self, url: str, webhook: PaymentWebhook) -> None:
        body = webhook.model_dump_json().encode()
        timestamp = str(int(self._clock().timestamp()))
        headers = {
            "Content-Type": "application/json",
            ID_HEADER: str(webhook.payment_id),
            TIMESTAMP_HEADER: timestamp,
            SIGNATURE_HEADER: sign(self._secret, timestamp, body),
        }
        try:
            # httpx timeouts apply to each read; a server that trickles bytes could hold the
            # handler for ever. This is the deadline for the whole request.
            async with (
                asyncio.timeout(self._timeout),
                # Streaming and never reading the body: only the status code matters, and a
                # huge response is not loaded into memory.
                self._client.stream("POST", url, content=body, headers=headers) as response,
            ):
                status = response.status_code
        except (httpx.HTTPError, TimeoutError) as exc:
            raise WebhookDeliveryError(f"{type(exc).__name__}: {exc}") from exc
        if not 200 <= status < 300:
            # 5xx and 429 say "the server cannot take it now"; other codes come from a
            # server that is up. Both are retried, only the former trips the breaker.
            server_down = status >= 500 or status == 429
            raise WebhookDeliveryError(f"HTTP {status}", server_down=server_down)


def create_http_client(timeout_seconds: float) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=httpx.Timeout(timeout_seconds, connect=min(timeout_seconds, 3.0)),
        # A redirect could lead the request to an internal address; the client gets one URL.
        follow_redirects=False,
        headers={"User-Agent": "payment-processing-webhooks/0.1"},
    )
