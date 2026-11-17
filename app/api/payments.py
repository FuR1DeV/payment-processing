from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Response, status

from app.services.payments import CreatePaymentCommand, IdempotencyKeyReusedError

from .dependencies import ContainerDep, require_api_key
from .schemas import CreatePaymentRequest, ErrorOut, PaymentAccepted, PaymentOut

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(require_api_key)],
    responses={401: {"model": ErrorOut, "description": "Missing or invalid X-API-Key"}},
)


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    responses={
        400: {"model": ErrorOut, "description": "No Idempotency-Key header"},
        422: {"model": ErrorOut, "description": "The key was used with a different body"},
    },
)
async def create_payment(
    body: CreatePaymentRequest,
    response: Response,
    container: ContainerDep,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key", max_length=255)] = None,
) -> PaymentAccepted:
    """Accept a payment for processing. The result arrives later at webhook_url.

    A repeated request with the same Idempotency-Key and body returns the same payment
    (its current status, header Idempotent-Replayed: true) and creates nothing new.
    """
    if idempotency_key is None or not idempotency_key.strip():
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST,
            detail={"code": "idempotency_key_required", "message": "Idempotency-Key header"},
        )
    command = CreatePaymentCommand(
        idempotency_key=idempotency_key,
        amount=body.amount,
        currency=body.currency,
        description=body.description,
        metadata=body.metadata,
        webhook_url=str(body.webhook_url),
    )
    try:
        result = await container.create_payment(command)
    except IdempotencyKeyReusedError:
        raise HTTPException(
            422,
            detail={
                "code": "idempotency_key_reused",
                "message": "this Idempotency-Key was used with a different request body",
            },
        ) from None

    response.headers["Location"] = f"{router.prefix}/{result.payment.id}"
    if not result.created:
        response.headers["Idempotent-Replayed"] = "true"
    return PaymentAccepted.of(result.payment)


@router.get("/{payment_id}", response_model=PaymentOut, responses={404: {"model": ErrorOut}})
async def get_payment(payment_id: UUID, container: ContainerDep) -> PaymentOut:
    """Payment details: status, processing time, whether the webhook was delivered."""
    payment = await container.queries.get(payment_id)
    if payment is None:
        raise HTTPException(
            status.HTTP_404_NOT_FOUND, detail={"code": "not_found", "message": "no such payment"}
        )
    return PaymentOut.of(payment)
