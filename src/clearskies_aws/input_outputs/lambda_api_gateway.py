from __future__ import annotations

from typing import Any

from clearskies.configs import String
from clearskies.input_outputs import Headers

from clearskies_aws.input_outputs import lambda_input_output


class LambdaApiGateway(lambda_input_output.LambdaInputOutput):
    """API Gateway v1 and v2 Lambda input/output handler."""

    resource = String(default="")

    def __init__(self, event: dict, context: dict[str, Any]):
        # Call parent constructor
        super().__init__(event, context)

        # Determine API Gateway version and parse accordingly
        version = self._detect_version(event)
        if version == "1.0":
            self._parse_event_v1(event)
        elif version == "2.0":
            self._parse_event_v2(event)
        else:
            raise ValueError(f"Unsupported API Gateway event version: {version}")

    def _detect_version(self, event: dict) -> str:
        """Detect API Gateway version from event structure."""
        if "version" in event:
            return event["version"]
        elif "httpMethod" in event:
            return "1.0"  # v1 has httpMethod at root level
        elif "requestContext" in event and "http" in event["requestContext"]:
            return "2.0"  # v2 has http in requestContext
        else:
            raise ValueError("Unable to determine API Gateway version from event structure")

    def _parse_event_v1(self, event: dict) -> None:
        """Parse API Gateway v1 event structure."""
        self.request_method = event.get("httpMethod", "GET").upper()
        self.path = event.get("path", "/")
        self.resource = event.get("resource", "")

        # Extract query parameters (v1 has both single and multi-value)
        self.query_parameters = {
            **(event.get("queryStringParameters") or {}),
            **(event.get("multiValueQueryStringParameters") or {}),
        }

        # Extract headers (v1 has both single and multi-value)
        headers_dict = {}
        for key, value in {
            **event.get("headers", {}),
            **event.get("multiValueHeaders", {}),
        }.items():
            headers_dict[key.lower()] = str(value)

        self.request_headers = Headers(headers_dict)

    def _parse_event_v2(self, event: dict) -> None:
        """Parse API Gateway v2 event structure."""
        request_context = event.get("requestContext", {})
        http_context = request_context.get("http", {})

        self.request_method = http_context.get("method", "GET").upper()
        self.path = http_context.get("path", "/")
        # v2 doesn't have resource field
        self.resource = ""

        # Extract query parameters (v2 only has single values)
        self.query_parameters = event.get("queryStringParameters") or {}

        # Extract headers (v2 only has single value headers)
        headers_dict = {}
        for key, value in event.get("headers", {}).items():
            headers_dict[key.lower()] = str(value)

        self.request_headers = Headers(headers_dict)

    def get_client_ip(self) -> str:
        """Get the client IP address from API Gateway event."""
        request_context = self.event.get("requestContext", {})

        # Try v1 format first (identity.sourceIp)
        identity = request_context.get("identity", {})
        if "sourceIp" in identity:
            return identity["sourceIp"]

        # Try v2 format (http.sourceIp)
        http_context = request_context.get("http", {})
        if "sourceIp" in http_context:
            return http_context["sourceIp"]

        raise ValueError("Unable to find the client ip inside the API Gateway")

    def get_protocol(self) -> str:
        """Get the protocol from API Gateway request context."""
        request_context = self.event.get("requestContext", {})

        # Try v2 format first (has explicit protocol)
        http_context = request_context.get("http", {})
        if "protocol" in http_context:
            protocol = http_context["protocol"]
            return "https" if protocol.upper().startswith("HTTPS") else "http"

        # v1 defaults to HTTPS
        return "https"

    def context_specifics(self) -> dict[str, Any]:
        """Provide API Gateway specific context data."""
        request_context = self.event.get("requestContext", {})
        http_context = request_context.get("http", {})

        return {
            **super().context_specifics(),
            "resource": self.resource,
            "stage": request_context.get("stage"),
            "request_id": request_context.get("requestId"),
            "api_id": request_context.get("apiId"),
            "api_version": self._detect_version(self.event),
        }
