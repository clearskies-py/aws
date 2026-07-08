from __future__ import annotations

from typing import TYPE_CHECKING

from clearskies_aws.di.inject.client import Client

if TYPE_CHECKING:
    from types_boto3_sqs import SQSClient as Boto3SQSClient

    from clearskies_aws.clients import BaseAwsClient


class SqsClient(Client):
    """
    Injectable for AWS SQS client.

    Usage:
        # Use factory defaults
        class MyClass:
            sqs_client = inject.SqsClient()

        # Override via bindings
        bindings = {
            "sqs_client": clearskies_aws.clients.SqsClient(region_name="us-east-1")
        }
    """

    @property
    def client_class(self) -> type[BaseAwsClient]:
        from clearskies_aws.clients import SqsClient

        return SqsClient

    def __get__(self, instance, parent) -> Boto3SQSClient:
        if instance is None:
            return self  # type: ignore

        return self.build_client(instance)
