from __future__ import annotations

import json
from typing import Any

from clearskies.exceptions import ClientError
from clearskies.input_outputs import Headers

from clearskies_aws.input_outputs import lambda_input_output


class LambdaSns(lambda_input_output.LambdaInputOutput):
    """SNS specific Lambda input/output handler."""

    record: dict[str, Any]

    def __init__(self, event: dict, context: dict[str, Any], url: str = "", request_method: str = "POST"):
        # Call parent constructor
        super().__init__(event, context)

        # SNS specific initialization
        self.path = url
        self.request_method = request_method.upper()

        # SNS events don't have query parameters or path parameters
        self.query_parameters = {}

        # SNS events don't have headers
        self.request_headers = Headers({})

        # Extract SNS message from event
        try:
            record = event["Records"][0]["Sns"]["Message"]
            self.record = json.loads(record)
        except (KeyError, IndexError, json.JSONDecodeError) as e:
            raise ClientError(
                "The message from AWS was not a valid SNS event with serialized JSON. "
                "The lambda_sns context for clearskies only accepts serialized JSON."
            )

    def respond(self, body: Any, status_code: int = 200) -> dict[str, Any]:
        """SNS events don't return responses."""
        return {}

    def get_body(self) -> str:
        """Get the SNS message as a JSON string."""
        return json.dumps(self.record) if self.record else ""

    def has_body(self) -> bool:
        """Check if SNS message exists."""
        return bool(self.record)

    def get_client_ip(self) -> str:
        """SNS events don't have client IP information."""
        return "127.0.0.1"

    def get_protocol(self) -> str:
        """SNS events don't have a protocol."""
        return "sns"

    def get_full_path(self) -> str:
        """Return the configured path."""
        return self.path

    def context_specifics(self) -> dict[str, Any]:
        """Provide SNS specific context data."""
        sns_record = self.event.get("Records", [{}])[0].get("Sns", {})

        return {
            **super().context_specifics(),
            "message_id": sns_record.get("MessageId"),
            "topic_arn": sns_record.get("TopicArn"),
            "subject": sns_record.get("Subject"),
            "timestamp": sns_record.get("Timestamp"),
        }

    @property
    def request_data(self) -> dict[str, Any] | list[Any] | None:
        """Return the SNS message data directly."""
        return self._record
