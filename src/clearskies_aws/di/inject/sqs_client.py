from __future__ import annotations

from typing import TYPE_CHECKING

from clearskies.di.injectable import Injectable

if TYPE_CHECKING:
    from types_boto3_sqs import SQSClient as Boto3SQSClient


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

    def __get__(self, instance, parent) -> Boto3SQSClient:
        if instance is None:
            return self  # type: ignore

        # Build the SqsClient Configurable class, which may be bound
        sqs_client_wrapper = self._di.build("sqs_client", cache=True)

        # Call it to get the boto3 SQSClient
        return sqs_client_wrapper()
