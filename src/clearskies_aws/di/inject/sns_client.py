from __future__ import annotations

from typing import TYPE_CHECKING

from clearskies.di.injectable import Injectable

if TYPE_CHECKING:
    from types_boto3_sns import SNSClient as Boto3SNSClient

from clearskies_aws.clients import SnsClient

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

    client_class = SnsClient

    def __get__(self, instance, parent) -> Boto3SNSClient:
        if parent is None:
            return instance # type: ignore

        return self.build_client() # type: ignore
