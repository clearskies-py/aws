from __future__ import annotations

from typing import TYPE_CHECKING

from clearskies.di.injectable import Injectable

if TYPE_CHECKING:
    from types_boto3_sns import SNSClient as Boto3SNSClient


class SnsClient(Injectable):
    """
    Injectable for AWS SNS client.

    Usage:
        # Use factory defaults
        class MyClass:
            sns_client = inject.SnsClient()

        # Override via bindings
        bindings = {
            "sns_client": clearskies_aws.clients.SnsClient(region_name="us-east-1")
        }
    """

    def __get__(self, instance, parent) -> Boto3SNSClient:
        if instance is None:
            return self  # type: ignore

        # Build the SnsClient Configurable class, which may be bound
        sns_client_wrapper = self._di.build("sns_client", cache=True)

        # Call it to get the boto3 SNSClient
        return sns_client_wrapper()
