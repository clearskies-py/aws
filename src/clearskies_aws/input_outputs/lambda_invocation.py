from __future__ import annotations

import json
from typing import Any

from clearskies.exceptions import ClientError
from clearskies.input_outputs import Headers

from clearskies_aws.input_outputs import lambda_input_output


class LambdaInvocation(lambda_input_output.LambdaInputOutput):
    """Direct Lambda invocation specific input/output handler."""

    def __init__(
        self,
        event: dict[str, Any],
        context: dict[str, Any],
        method: str = "POST",
        url: str = "",
    ):
        # Call parent constructor
        super().__init__(event, context)

        # Direct invocation specific initialization
        self.path = url
        self.request_method = method.upper()

        # Direct invocations don't have query parameters or path parameters
        self.query_parameters = {}

        # Direct invocations don't have headers
        self.request_headers = Headers({})

    def has_body(self) -> bool:
        """Direct invocations always have a body (the event itself)."""
        return True

    def get_body(self) -> str:
        """Get the entire event as the body."""
        if isinstance(self.event, (dict, list)):
            return json.dumps(self.event)
        return str(self.event)

    def respond(self, body: Any, status_code: int = 200) -> Any:
        """Return the response directly for direct invocations."""
        if isinstance(body, bytes):
            return body.decode("utf-8")
        return body

    def get_client_ip(self) -> str:
        """Direct invocations don't have client IP information."""
        return "127.0.0.1"

    def get_protocol(self) -> str:
        """Direct invocations don't have a protocol."""
        return "lambda"

    def get_full_path(self) -> str:
        """Return the configured path."""
        return self.path

    def context_specifics(self) -> dict[str, Any]:
        """Provide direct invocation specific context data."""
        return {
            **super().context_specifics(),
            "invocation_type": "direct",
            "function_name": self.context.get("function_name"),
            "function_version": self.context.get("function_version"),
            "request_id": self.context.get("aws_request_id"),
        }

    @property
    def request_data(self) -> dict[str, Any] | list[Any] | None:
        """Return the event directly as request data."""
        return self.event

    def json_body(
        self, required: bool = True, allow_non_json_bodies: bool = False
    ) -> dict[str, Any] | list[Any] | None:
        """Get the event as JSON data."""
        # For direct invocations, the event is already an object, not a JSON string
        if required and not self.event:
            raise ClientError("Request body was not valid JSON")
        return self.event
