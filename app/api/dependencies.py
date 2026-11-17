import secrets
from typing import Annotated

from fastapi import Depends, HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader

from app.bootstrap import ApiContainer


def get_container(request: Request) -> ApiContainer:
    return request.app.state.container


ContainerDep = Annotated[ApiContainer, Depends(get_container)]

# auto_error=False: a missing key gets the same 401 as a wrong one, not FastAPI's 403.
_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def require_api_key(
    container: ContainerDep,
    api_key: Annotated[str | None, Security(_api_key_header)],
) -> None:
    expected = container.settings.api_key.get_secret_value().encode()
    # compare_digest takes the same time wherever the strings differ: no timing hints.
    if api_key is None or not secrets.compare_digest(api_key.encode(), expected):
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            detail={"code": "unauthorized", "message": "missing or invalid X-API-Key"},
            headers={"WWW-Authenticate": "ApiKey"},
        )
