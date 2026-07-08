"""SNS client wrapper for clearskies DI."""

from __future__ import annotations

from types_boto3_sns import SNSClient as Boto3SNSClient

from clearskies_aws.clients.base_aws_client import BaseAwsClient


class SnsClient(BaseAwsClient):
    """
    Publish messages to Amazon SNS topics.

    Provides a configurable wrapper around boto3's SNS client for clearskies dependency
    injection. Supports region configuration, role assumption, and client caching through
    inherited [`BaseAwsClient`](base_aws_client.py) configuration options.
    """

    cached_client: Boto3SNSClient
    service_name = "sns"

    def __call__(self) -> Boto3SNSClient:
        """
        Get or create the SNS client.

        Returns a cached client if caching is enabled, otherwise creates a new one.

        Example:
            Direct instantiation

            ```python
            from clearskies_aws.clients import SnsClient

            sns = SnsClient(region_name="us-west-2")
            client = sns()
            client.publish(TopicArn="arn:aws:sns:us-west-2:123456789012:my-topic", Message="Hello, World!")
            ```

        Example:
            Injectable pattern in an action

            ```python
            from clearskies.di.injectable_properties import InjectableProperties
            from clearskies_aws.di import inject
            from clearskies import Model


            class MyAction(InjectableProperties):
                sns = inject.SnsClient()

                def __call__(self, model: Model):
                    self.sns().publish(TopicArn=self.topic_arn, Message=model.to_json())
            ```

        Example:
            Publishing with message attributes

            ```python
            from clearskies_aws.clients import SnsClient

            sns = SnsClient(region_name="us-east-1")
            client = sns()

            client.publish(
                TopicArn="arn:aws:sns:us-east-1:123/topic",
                Message="Order completed",
                MessageAttributes={
                    "order_id": {"DataType": "String", "StringValue": "12345"},
                    "status": {"DataType": "String", "StringValue": "completed"},
                },
            )
            ```
        """
        if self.cache and hasattr(self, "cached_client"):
            return self.cached_client

        self.cached_client: Boto3SNSClient = self.create_client("sns")
        return self.cached_client
