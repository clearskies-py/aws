from __future__ import annotations

from typing import Any

from clearskies.configs import String
from clearskies.input_outputs import Headers

from clearskies_aws.input_outputs import lambda_input_output


class LambdaAPIGateway(lambda_input_output.LambdaInputOutput):
    """API Gateway v1 specific Lambda input/output handler."""

    resource = String(default="")

    def __init__(self, event: dict[str, Any], context: dict[str, Any]):
        # Call parent constructor
        super().__init__(event, context)

        # API Gateway v1 specific initialization
        self.request_method = event.get("httpMethod", "GET").upper()
        self.resource = event.get("resource", "")
        self.path = event.get("path", "/")

        # Extract query parameters (API Gateway v1 has both single and multi-value)
        self.query_parameters = {
            **(event.get("queryStringParameters") or {}),
            **(event.get("multiValueQueryStringParameters") or {}),
        }

        # Extract path parameters as routing_data (clearskies convention)
        self.routing_data = event.get("pathParameters") or {}

        # Extract headers (API Gateway v1 has both single and multi-value)
        headers_dict = {}
        for key, value in {
            **event.get("headers", {}),
            **event.get("multiValueHeaders", {}),
        }.items():
            headers_dict[key.lower()] = str(value)

        self.request_headers = Headers(headers_dict)

    def get_client_ip(self) -> str:
        """Get client IP address from request context or headers."""
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

    def context_specifics(self) -> dict[str, Any]:
        """Extend parent context with API Gateway specific data."""
        request_context = self.event.get("requestContext", {})
        return {
            **super().context_specifics(),
            "resource": self.resource,
            "path": self.path,
            "stage": request_context.get("stage"),
            "request_id": request_context.get("requestId"),
            "api_id": request_context.get("apiId"),
        }
