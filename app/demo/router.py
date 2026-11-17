"""Helper endpoints of the demo page. The same X-API-Key as the public API; switched off
together with the page (DEMO_PAGE_ENABLED=false)."""

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from app.api.dependencies import ContainerDep, require_api_key
from app.api.schemas import ErrorOut

from .tools import DemoBackendError, DemoTools

router = APIRouter(
    prefix="/demo/api",
    tags=["demo"],
    dependencies=[Depends(require_api_key)],
    responses={401: {"model": ErrorOut}, 502: {"model": ErrorOut}},
)


def get_tools(container: ContainerDep) -> DemoTools:
    if container.demo is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND)
    return container.demo


ToolsDep = Annotated[DemoTools, Depends(get_tools)]


class OutboxEventOut(BaseModel):
    event_id: UUID
    event_type: str
    created_at: datetime
    published_at: datetime | None
    attempts: int
    last_error: str | None


class PaymentRowOut(BaseModel):
    id: UUID
    amount: Decimal
    currency: str
    status: str
    failure_reason: str | None
    idempotency_key: str
    created_at: datetime
    processed_at: datetime | None
    webhook_delivered_at: datetime | None


class OutboxRowOut(BaseModel):
    id: int
    event_id: UUID
    event_type: str
    aggregate_id: UUID
    created_at: datetime
    published_at: datetime | None
    attempts: int
    next_attempt_at: datetime
    last_error: str | None


class DbOut(BaseModel):
    payments: list[PaymentRowOut]
    payments_by_status: dict[str, int]
    outbox: list[OutboxRowOut]
    outbox_total: int
    outbox_unpublished: int


class QueueOut(BaseModel):
    name: str
    role: str
    delay_ms: int | None
    messages: int
    consumers: int


class Mode(BaseModel):
    fail: bool


def _unavailable(exc: DemoBackendError) -> HTTPException:
    return HTTPException(
        status.HTTP_502_BAD_GATEWAY, detail={"code": "unavailable", "message": str(exc)}
    )


@router.get("/payments/{payment_id}/outbox", response_model=list[OutboxEventOut])
async def payment_outbox(payment_id: UUID, tools: ToolsDep) -> list[OutboxEventOut]:
    """Outbox events of the payment: when written, when published by the relay."""
    events = await tools.outbox_events(payment_id)
    return [OutboxEventOut.model_validate(e, from_attributes=True) for e in events]


@router.get("/db", response_model=DbOut)
async def tables(tools: ToolsDep, limit: Annotated[int, Query(ge=1, le=50)] = 10) -> DbOut:
    """The latest rows of payments and outbox as they are in PostgreSQL, plus counts."""
    view = await tools.tables(limit)
    return DbOut(
        payments=[PaymentRowOut.model_validate(p, from_attributes=True) for p in view.payments],
        payments_by_status=view.payments_by_status,
        outbox=[OutboxRowOut.model_validate(e, from_attributes=True) for e in view.outbox],
        outbox_total=view.outbox_total,
        outbox_unpublished=view.outbox_unpublished,
    )


@router.get("/queues", response_model=list[QueueOut])
async def queues(tools: ToolsDep) -> list[QueueOut]:
    """Messages waiting in each queue right now."""
    try:
        return [QueueOut(**vars(q)) for q in await tools.queues()]
    except DemoBackendError as exc:
        raise _unavailable(exc) from None


@router.post("/dlq/replay")
async def replay_dlq(tools: ToolsDep) -> dict[str, int]:
    """Same as python -m app.messaging.dlq --replay."""
    try:
        return {"moved": await tools.replay_dlq()}
    except DemoBackendError as exc:
        raise _unavailable(exc) from None


@router.get("/receiver/received")
async def receiver_log(tools: ToolsDep, payment_id: UUID | None = None) -> list[dict[str, Any]]:
    """What the demo webhook receiver got, newest first."""
    params = {"payment_id": str(payment_id)} if payment_id else None
    try:
        return await tools.receiver("GET", "/received", params=params)
    except DemoBackendError as exc:
        raise _unavailable(exc) from None


@router.get("/receiver/mode", response_model=Mode)
async def receiver_mode(tools: ToolsDep) -> Mode:
    try:
        return Mode(**await tools.receiver("GET", "/mode"))
    except DemoBackendError as exc:
        raise _unavailable(exc) from None


@router.put("/receiver/mode", response_model=Mode)
async def set_receiver_mode(mode: Mode, tools: ToolsDep) -> Mode:
    """fail=true: the receiver answers 500 to every webhook, as if the client's server were
    down."""
    try:
        return Mode(**await tools.receiver("PUT", "/mode", json=mode.model_dump()))
    except DemoBackendError as exc:
        raise _unavailable(exc) from None
