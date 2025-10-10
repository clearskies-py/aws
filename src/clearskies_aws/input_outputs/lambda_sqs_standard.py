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
        self, record: str, event: dict[str, Any], context: dict[str, Any], url: str = "", method: str = "POST"
    ):
        # Call parent constructor with the full event
        super().__init__(event, context)

        # Store the individual SQS record
        self.record = json.loads(record)
        print("SQS record:", self.record)
        # SQS specific initialization
        self.path = url if url else "/"
        self.request_method = method.upper() if method else "POST"

        # SQS events don't have query parameters or path parameters
        self.query_parameters = {}

        # SQS events don't have headers
        self.request_headers = Headers({})

    def respond(self, body: Any, status_code: int = 200) -> dict[str, Any]:
        """SQS events don't return responses."""
        return {}

    def get_body(self) -> str:
        """Get the SQS message body."""
        return json.dumps(self.record)

    def has_body(self) -> bool:
        """Check if SQS message has a body."""
        return True

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
            "sqs_message_id": self.record.get("messageId"),
            "sqs_receipt_handle": self.record.get("receiptHandle"),
            "sqs_source_arn": self.record.get("eventSourceARN"),
            "sqs_sent_timestamp": self.record.get("attributes", {}).get("SentTimestamp"),
            "sqs_approximate_receive_count": self.record.get("attributes", {}).get("ApproximateReceiveCount"),
            "sqs_message_attributes": self.record.get("messageAttributes", {}),
            "sqs_record": self.record,
        }

    @property
    def request_data(self) -> dict[str, Any]:
        """Return the SQS message body as parsed JSON."""
        body = self.get_body()
        if not body:
            return {}
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            raise ClientError("SQS message body was not valid JSON")
