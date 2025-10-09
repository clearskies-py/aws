from __future__ import annotations

from typing import Any

from clearskies.configs import String
from clearskies.input_outputs import Headers

from clearskies_aws.input_outputs import lambda_input_output


class LambdaAPIGatewayWebSocket(lambda_input_output.LambdaInputOutput):
    """API Gateway WebSocket specific Lambda input/output handler."""

    route_key = String(default="")
    connection_id = String(default="")

    def __init__(self, event: dict[str, Any], context: dict[str, Any]):
        # Call parent constructor
        super().__init__(event, context)

        # WebSocket specific initialization
        request_context = event.get("requestContext", {})

        # WebSocket uses route_key instead of HTTP method
        self.route_key = request_context.get("routeKey", "")
        self.request_method = self.route_key.upper()  # For compatibility

        # WebSocket connection ID
        self.connection_id = request_context.get("connectionId", "")

        # WebSocket events typically don't have query parameters or path parameters
        self.query_parameters = event.get("queryStringParameters") or {}

        # Extract headers
        headers_dict = {}
        for key, value in event.get("headers", {}).items():
            headers_dict[key.lower()] = str(value)

        self.request_headers = Headers(headers_dict)

    def get_client_ip(self) -> str:
        """Get the client IP address from WebSocket request context."""
        request_context = self.event.get("requestContext", {})
        identity = request_context.get("identity", {})

        if "sourceIp" in identity:
            return identity["sourceIp"]

        # Fall back to X-Forwarded-For header
        forwarded_for = self.request_headers.get("x-forwarded-for")
        if forwarded_for:
            return forwarded_for

        # Final fallback
        return "127.0.0.1"

    def respond(self, body: Any, status_code: int = 200) -> dict[str, Any]:
        """Create WebSocket specific response format."""
        # WebSocket responses are simpler than HTTP responses
        return {
            "statusCode": status_code,
        }

    def context_specifics(self) -> dict[str, Any]:
        """Provide WebSocket specific context data."""
        request_context = self.event.get("requestContext", {})

        return {
            **super().context_specifics(),
            "connection_id": self.connection_id,
            "route_key": self.route_key,
            "stage": request_context.get("stage"),
            "request_id": request_context.get("requestId"),
            "api_id": request_context.get("apiId"),
            "domain_name": request_context.get("domainName"),
            "event_type": request_context.get("eventType"),
            "connected_at": request_context.get("connectedAt"),
        }
