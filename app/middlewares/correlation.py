import uuid
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp
from structlog.contextvars import bind_contextvars, clear_contextvars


class CorrelationMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp, header_name: str = "X-Correlation-ID"):
        super().__init__(app)
        self.header_name = header_name

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        correlation_id = request.headers.get(self.header_name) or str(uuid.uuid4())
        bind_contextvars(correlation_id=correlation_id)

        try:
            response = await call_next(request)
        finally:
            clear_contextvars()

        response.headers[self.header_name] = correlation_id
        return response
