from __future__ import annotations

from typing import TYPE_CHECKING

from clearskies.di.injectable import Injectable

if TYPE_CHECKING:
    from types_boto3_sqs import SQSClient as Boto3SQSClient
    from clearskies_aws.clients import BaseAwsClient

class SqsClient(Injectable):
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
        if parent is None:
            return instance # type: ignore

        return self.build_client() # type: ignore
