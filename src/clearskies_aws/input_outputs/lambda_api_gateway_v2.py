from __future__ import annotations

from typing import Any

from clearskies.input_outputs import Headers

from clearskies_aws.input_outputs import lambda_input_output


class LambdaAPIGatewayV2(lambda_input_output.LambdaInputOutput):
    """API Gateway v2 specific Lambda input/output handler."""

    def __init__(self, event: dict[str, Any], context: dict[str, Any]):
        # Call parent constructor
        super().__init__(event, context)

        # API Gateway v2 specific initialization
        request_context = event.get("requestContext", {})
        http_context = request_context.get("http", {})

        self.request_method = http_context.get("method", "GET").upper()
        self.path = http_context.get("path", "/")

        # Extract query parameters (API Gateway v2 only has single values)
        self.query_parameters = event.get("queryStringParameters") or {}

        # Extract path parameters as routing_data (clearskies convention)
        self.routing_data = event.get("pathParameters") or {}

        # Extract headers (API Gateway v2 only has single value headers)
        headers_dict = {}
        for key, value in event.get("headers", {}).items():
            headers_dict[key.lower()] = str(value)

        self.request_headers = Headers(headers_dict)

    def get_client_ip(self) -> str:
        """Get the client IP address from API Gateway v2 event."""
        request_context = self.event.get("requestContext", {})
        http_context = request_context.get("http", {})

        if "sourceIp" in http_context:
            return http_context["sourceIp"]

        # Fall back to X-Forwarded-For header
        forwarded_for = self.request_headers.get("x-forwarded-for")
        if forwarded_for:
            return forwarded_for

        # Final fallback
        return "127.0.0.1"

    def get_protocol(self) -> str:
        """Get the protocol from API Gateway v2 request context."""
        request_context = self.event.get("requestContext", {})
        http_context = request_context.get("http", {})
        protocol = http_context.get("protocol", "HTTP/1.1")

        # Return just the protocol part (https/http)
        return "https" if protocol.startswith("HTTPS") else "http"

    def context_specifics(self) -> dict[str, Any]:
        """Provide API Gateway v2 specific context data."""
        request_context = self.event.get("requestContext", {})
        http_context = request_context.get("http", {})

        return {
            **super().context_specifics(),
            "path": self.path,
            "stage": request_context.get("stage"),
            "request_id": request_context.get("requestId"),
            "api_id": request_context.get("apiId"),
            "domain_name": request_context.get("domainName"),
            "protocol": http_context.get("protocol"),
            "user_agent": http_context.get("userAgent"),
        }
