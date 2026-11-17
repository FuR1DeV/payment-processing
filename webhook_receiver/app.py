"""Stand-in for the client's server that receives payment webhooks. Demo only.

POST /webhook    takes a webhook, checks the signature; answers 200, or 500 in failure mode
GET  /received   what came in, newest first (?payment_id= filters)
GET  /mode       current mode
PUT  /mode       {"fail": true} makes every webhook fail with 500: retries and the DLQ
                 can be watched live; {"fail": false} switches it back
"""

import json
import os
from collections import deque
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from app.integrations.webhooks import ID_HEADER, SIGNATURE_HEADER, TIMESTAMP_HEADER, verify


class Mode(BaseModel):
    fail: bool


def create_app(secret: str | None = None) -> FastAPI:
    key = (secret if secret is not None else os.environ.get("WEBHOOK_SECRET", "")).encode()
    mode = Mode(fail=False)
    received: deque[dict[str, Any]] = deque(maxlen=1000)
    app = FastAPI(title="Webhook receiver (demo)")

    @app.post("/webhook")
    async def webhook(request: Request) -> JSONResponse:
        body = await request.body()
        signature_valid = verify(
            key,
            request.headers.get(TIMESTAMP_HEADER, ""),
            body,
            request.headers.get(SIGNATURE_HEADER, ""),
            now=datetime.now(UTC),
        )
        answer = 200 if signature_valid else 401
        if mode.fail:
            answer = 500
        try:
            status = json.loads(body).get("status")
        except (ValueError, AttributeError):
            status = None
        received.appendleft(
            {
                "received_at": datetime.now(UTC).isoformat(timespec="milliseconds"),
                "payment_id": request.headers.get(ID_HEADER),
                "status": status,
                "signature_valid": signature_valid,
                "answered": answer,
            }
        )
        return JSONResponse({"accepted": answer == 200}, status_code=answer)

    @app.get("/received")
    async def list_received(payment_id: UUID | None = None) -> list[dict[str, Any]]:
        if payment_id is None:
            return list(received)
        return [r for r in received if r["payment_id"] == str(payment_id)]

    @app.get("/mode")
    async def get_mode() -> Mode:
        return mode

    @app.put("/mode")
    async def set_mode(new: Mode) -> Mode:
        mode.fail = new.fail
        return mode

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app


app = create_app()
