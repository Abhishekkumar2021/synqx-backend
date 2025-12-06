import uuid
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response
from starlette.types import ASGIApp

class CorrelationMiddleware(BaseHTTPMiddleware):
    """
    Middleware to ensure every request has a Correlation-ID header.
    If not present, generates a new UUID.
    """
    
    def __init__(self, app: ASGIApp, header_name: str = "X-Correlation-ID"):
        super().__init__(app)
        self.header_name = header_name

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        correlation_id = request.headers.get(self.header_name)
        if not correlation_id:
            correlation_id = str(uuid.uuid4())
        
        # You might want to set this in a context var here for logging
        
        response = await call_next(request)
        response.headers[self.header_name] = correlation_id
        return response
