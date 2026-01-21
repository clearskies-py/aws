from __future__ import annotations

import json
from typing import Any as TypingAny
from typing import Callable

from clearskies.configs import Any
from clearskies.exceptions import ClientError
from clearskies.input_outputs import Headers

from clearskies_aws.input_outputs import lambda_input_output


class LambdaStepFunction(lambda_input_output.LambdaInputOutput):
    """Step Functions specific input/output handler.

    Handles Lambda invocations from AWS Step Functions, with support for
    extracting assigned variables and exposing them through the clearskies
    Environment class.
    """

    environment_keys = Any(default=None)

    def __init__(
        self,
        event: dict[str, TypingAny],
        context: dict[str, TypingAny],
        request_method: str = "",
        url: str = "",
        environment_keys: list[str] | dict[str, str] | Callable[[dict[str, TypingAny]], dict[str, str]] | None = None,
    ):
        super().__init__(event, context)

        self.environment_keys = environment_keys
        self._extracted_environment: dict[str, str] = {}

        self._extract_environment_variables()

        if url:
            self.url = url
            self.path = url
        else:
            self.supports_url = True
        if request_method:
            self.request_method = request_method.upper()
        else:
            self.supports_request_method = False

        self.request_headers = Headers({})

    def _extract_environment_variables(self) -> None:
        """Extract environment variables from the event based on configuration."""
        if self.environment_keys is None:
            return

        if callable(self.environment_keys):
            result = self.environment_keys(self.event)
            if result:
                self._extracted_environment = {k: str(v) for k, v in result.items() if v is not None}
        elif isinstance(self.environment_keys, dict):
            for event_key, env_name in self.environment_keys.items():
                if event_key in self.event:
                    value = self.event[event_key]
                    if value is not None:
                        self._extracted_environment[env_name] = str(value)
        elif isinstance(self.environment_keys, list):
            for key in self.environment_keys:
                if key in self.event:
                    value = self.event[key]
                    if value is not None:
                        self._extracted_environment[key] = str(value)

    @property
    def extracted_environment(self) -> dict[str, str]:
        """Return the extracted environment variables."""
        return self._extracted_environment

    def has_body(self) -> bool:
        """Step Functions invocations always have a body - the event itself."""
        return True

    def get_body(self) -> str:
        """Get the entire event as the body (JSON-encoded)."""
        if isinstance(self.event, (dict, list)):
            return json.dumps(self.event)
        return str(self.event)

    def respond(self, body: TypingAny, status_code: int = 200) -> TypingAny:
        """Return the response directly for Step Functions invocations."""
        if isinstance(body, bytes):
            return body.decode("utf-8")
        return body

    def get_protocol(self) -> str:
        """Step Functions invocations use a custom protocol."""
        return "step-functions"

    def context_specifics(self) -> dict[str, TypingAny]:
        """Provide Step Functions specific context data."""
        states_context = self.event.get("$states", {})

        return {
            **super().context_specifics(),
            "invocation_type": "step-functions",
            "function_name": self.context.get("function_name"),
            "function_version": self.context.get("function_version"),
            "request_id": self.context.get("aws_request_id"),
            "states_context": states_context,
            "extracted_environment": self._extracted_environment,
        }

    @property
    def request_data(self) -> dict[str, TypingAny] | list[TypingAny] | None:
        """Return the event directly as request data."""
        return self.event

    def json_body(
        self, required: bool = True, allow_non_json_bodies: bool = False
    ) -> dict[str, TypingAny] | list[TypingAny] | None:
        """Get the event as JSON data (already parsed)."""
        if required and not self.event:
            raise ClientError("Request body was not valid JSON")
        return self.event
