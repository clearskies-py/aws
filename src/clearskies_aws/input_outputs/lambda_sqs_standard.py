from __future__ import annotations

import json
from typing import Any

from clearskies.exceptions import ClientError
from clearskies.input_outputs import Headers

from clearskies_aws.input_outputs import lambda_input_output


class LambdaSqsStandard(lambda_input_output.LambdaInputOutput):
    """SQS standard queue specific Lambda input/output handler."""

    def __init__(self, record: dict, event: dict, context: dict[str, Any], url: str = "", method: str = "POST"):
        # Call parent constructor with the full event
        super().__init__(event, context)

        # Store the individual SQS record
        self._record = record

        # SQS specific initialization
        self.path = url
        self.request_method = method.upper()

        # SQS events don't have query parameters or path parameters
        self.query_parameters = {}

        # SQS events don't have headers
        self.request_headers = Headers({})

    def respond(self, body: Any, status_code: int = 200) -> dict[str, Any]:
        """SQS events don't return responses."""
        return {}

    def get_body(self) -> str:
        """Get the SQS message body."""
        return self._record.get("body", "")

    def has_body(self) -> bool:
        """Check if SQS message has a body."""
        return bool(self._record.get("body"))

    def get_client_ip(self) -> str:
        """SQS events don't have client IP information."""
        return "127.0.0.1"

    def get_protocol(self) -> str:
        """SQS events don't have a protocol."""
        return "sqs"

    def get_full_path(self) -> str:
        """Return the configured path."""
        return self.path

    def context_specifics(self) -> dict[str, Any]:
        """Provide SQS specific context data."""
        return {
            **super().context_specifics(),
            "sqs_message_id": self._record.get("messageId"),
            "sqs_receipt_handle": self._record.get("receiptHandle"),
            "sqs_source_arn": self._record.get("eventSourceARN"),
            "sqs_sent_timestamp": self._record.get("attributes", {}).get("SentTimestamp"),
            "sqs_approximate_receive_count": self._record.get("attributes", {}).get("ApproximateReceiveCount"),
            "sqs_message_attributes": self._record.get("messageAttributes", {}),
            "sqs_record": self._record,
        }

    @property
    def request_data(self) -> dict[str, Any] | list[Any] | None:
        """Return the SQS message body as parsed JSON."""
        body = self.get_body()
        if not body:
            return None

        try:
            return json.loads(body)
        except json.JSONDecodeError:
            raise ClientError("SQS message body was not valid JSON")
