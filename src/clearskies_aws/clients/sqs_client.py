"""SQS client wrapper for clearskies DI."""

from __future__ import annotations

from types_boto3_sqs import SQSClient as Boto3SQSClient

from clearskies_aws.clients.base_aws_client import BaseAwsClient


class SqsClient(BaseAwsClient):
    """
    Send and receive messages with Amazon SQS queues.

    Provides a configurable wrapper around boto3's SQS client for clearskies dependency
    injection. Supports region configuration, role assumption, and client caching through
    inherited [`BaseAwsClient`](base_aws_client.py) configuration options.
    """

    def __call__(self) -> Boto3SQSClient:
        """
        Get or create the SQS client.

        Returns a cached client if caching is enabled, otherwise creates a new one.

        Example:
            Direct instantiation

            ```python
            from clearskies_aws.clients import SqsClient

            sqs = SqsClient(region_name="us-west-2")
            client = sqs()
            client.send_message(
                QueueUrl="https://sqs.us-west-2.amazonaws.com/123456789012/my-queue",
                MessageBody="Hello, World!",
            )
            ```

        Example:
            Injectable pattern in an action

            ```python
            from clearskies.di.injectable_properties import InjectableProperties
            from clearskies_aws.di import inject
            from clearskies import Model


            class MyAction(InjectableProperties):
                sqs = inject.SqsClient()

                def __call__(self, model: Model):
                    self.sqs().send_message(QueueUrl=self.queue_url, MessageBody=model.to_json())
            ```

        Example:
            Batch message operations

            ```python
            from clearskies_aws.clients import SqsClient

            sqs = SqsClient(region_name="us-east-1")
            client = sqs()

            # Send multiple messages
            entries = [
                {"Id": "1", "MessageBody": "Message 1"},
                {"Id": "2", "MessageBody": "Message 2"},
                {"Id": "3", "MessageBody": "Message 3"},
            ]
            client.send_message_batch(QueueUrl="https://sqs.us-east-1.amazonaws.com/123/queue", Entries=entries)
            ```
        """
        if self.cache and self.cached_client is not None:
            return self.cached_client  # type: ignore

        client = self.create_client("sqs")

        if self.cache:
            self.cached_client = client

        return client  # type: ignore
