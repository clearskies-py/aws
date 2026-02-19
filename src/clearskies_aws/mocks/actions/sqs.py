from __future__ import annotations

from typing import Any

from clearskies import Model

from clearskies_aws.actions.sqs import SQS as BaseSQS


class SQS(BaseSQS):
    calls: list[dict[str, Any]] | None = None

    @classmethod
    def mock(cls, di):
        cls.calls = []
        di.mock_class(BaseSQS, SQS)

    def __call__(self, model: Model) -> None:
        """Record SQS send message call without actually sending."""
        if SQS.calls is None:
            SQS.calls = []

        SQS.calls.append(
            {
                "QueueUrl": self.get_queue_url(model),
                "MessageBody": self.get_message_body(model),
            }
        )
