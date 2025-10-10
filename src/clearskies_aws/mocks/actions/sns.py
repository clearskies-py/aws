from __future__ import annotations

from typing import Any

from clearskies import Model
from types_boto3_sns import SNSClient

from clearskies_aws.actions.sns import SNS as BaseSNS


class SNS(BaseSNS):
    calls: list[dict[str, Any]] | None = None

    @classmethod
    def mock(cls, di):
        cls.calls = []
        di.mock_class(BaseSNS, SNS)

    def _execute_action(self, client: SNSClient, model: Model) -> None:
        """Send a notification as configured."""
        if SNS.calls is None:
            SNS.calls = []

        SNS.calls.append(
            {
                "TopicArn": self.get_topic_arn(model),
                "Message": self.get_message_body(model),
            }
        )
