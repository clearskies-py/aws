from __future__ import annotations

from typing import Any

from clearskies import Model
from types_boto3_sqs import SQSClient

from clearskies_aws.actions.sqs import SQS as BaseSQS


class SQS(BaseSQS):
    calls: list[dict[str, Any]] | None = None

    @classmethod
    def mock(cls, di):
        cls.calls = []
        di.mock_class(BaseSQS, SQS)

    def _execute_action(self, client: SQSClient, model: Model) -> None:
        """Send a notification as configured."""
        if SQS.calls is None:
            SQS.calls = []

        SQS.calls.append(
            {
                "QueueUrl": self.get_queue_url(model),
                "MessageBody": self.get_message_body(model),
            }
        )
