from __future__ import annotations

import json
from typing import Any

from clearskies.configs import AnyDict, String
from clearskies.exceptions import ClientError
from clearskies.input_outputs import Headers

from clearskies_aws.input_outputs import lambda_input_output


class LambdaSqsStandard(lambda_input_output.LambdaInputOutput):
    """SQS standard queue specific Lambda input/output handler."""

    record = AnyDict(default={})
    path = String(default="/")

    def __init__(
        self,
        record: dict[str, Any],
        event: dict[str, Any],
        context: dict[str, Any],
        url: str = "",
        request_method: str = "",
    ):
        # Call parent constructor with the full event
        super().__init__(event, context)

        # Store the individual SQS record
        self.record = record
        # SQS specific initialization
        if url:
            self.path = url
        else:
            self.supports_url = False

        if request_method:
            self.request_method = request_method.upper()
        else:
            self.supports_request_method = False

        # SQS events don't have headers
        self.request_headers = Headers({})

    def respond(self, body: Any, status_code: int = 200) -> None:
        """Respond to the client, but SQS has no client."""
        # since there is no response to the client, we want to raise an exception for any non-200 status code so
        # the lambda execution itself will be marked as a failure.
        if status_code > 299:
            if not isinstance(body, str):
                body = json.dumps(body)
            raise Exception(f"Non-200 Status code returned by application: {status_code}.  Response: '{body}'")

    def get_body(self) -> str:
        """Get request body with base64 decoding if needed."""
        return self.record["body"]

    def has_body(self) -> bool:
        """Check if SQS message has a body."""
        return True

    def get_client_ip(self) -> str:
        """SQS events don't have client IP information."""
        return "127.0.0.1"

    def get_protocol(self) -> str:
        """SQS events don't have a protocol."""
        return "sqs"

    def context_specifics(self) -> dict[str, Any]:
        """Provide SQS specific context data."""
        return {
            **super().context_specifics(),
            "message_id": self.record.get("messageId"),
            "receipt_handle": self.record.get("receiptHandle"),
            "source_arn": self.record.get("eventSourceARN"),
            "sent_timestamp": self.record.get("attributes", {}).get("SentTimestamp"),
            "approximate_receive_count": self.record.get("attributes", {}).get("ApproximateReceiveCount"),
            "message_attributes": self.record.get("messageAttributes", {}),
            "record": self.record,
        }
