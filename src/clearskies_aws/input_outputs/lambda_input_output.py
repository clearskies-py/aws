from __future__ import annotations

import base64
import json
from abc import abstractmethod
from typing import Any, cast
from urllib.parse import urlencode

from clearskies.configs import AnyDict, String
from clearskies.input_outputs import InputOutput


class LambdaInputOutput(InputOutput):
    """Base class for Lambda input/output handlers that provides common Lambda functionality."""

    event = AnyDict(default={})
    context = AnyDict(default={})
    path = String(default="/")

    _cached_body = None
    _body_was_cached = False

    def __init__(self, event: dict[str, Any], context: dict[str, Any]):
        # Store event and context
        self.event = event
        self.context = context

        # Initialize the base class
        super().__init__()

    def respond(self, body: Any, status_code: int = 200) -> dict[str, Any]:
        """Create standard Lambda HTTP response format."""
        if "content-type" not in self.response_headers:
            self.response_headers.content_type = "application/json; charset=UTF-8"

        is_base64 = False

        if isinstance(body, bytes):
            is_base64 = True
            final_body = base64.encodebytes(body).decode("utf8")
        elif isinstance(body, str):
            final_body = body
        else:
            final_body = json.dumps(body)

        return {
            "isBase64Encoded": is_base64,
            "statusCode": status_code,
            "headers": dict(self.response_headers),
            "body": final_body,
        }

    def has_body(self) -> bool:
        return bool(self.get_body())

    def get_body(self) -> str:
        """Get request body with base64 decoding if needed."""
        if not self._body_was_cached:
            self._body_was_cached = True
            self._cached_body = self.event.get("body", "")
            if (
                self._cached_body is not None
                and self.event.get("isBase64Encoded", False)
                and isinstance(self._cached_body, str)
            ):
                self._cached_body = base64.decodebytes(self._cached_body.encode("utf-8")).decode("utf-8")
        return self._cached_body or ""

    def get_client_ip(self) -> str:
        """Get client IP - can be overridden by subclasses for event-specific logic."""
        return "127.0.0.1"

    def get_protocol(self) -> str:
        """Get protocol."""
        # Default to HTTPS for most Lambda HTTP events
        return "https"

    def get_full_path(self) -> str:
        """Get full path."""
        return self.path

    def context_specifics(self) -> dict[str, Any]:
        """Provide Lambda-specific context."""
        return {
            "event": self.event,
            "context": self.context,
        }
