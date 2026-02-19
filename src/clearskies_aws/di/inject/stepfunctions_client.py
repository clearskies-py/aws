from __future__ import annotations

from typing import TYPE_CHECKING

from clearskies.di.injectable import Injectable

if TYPE_CHECKING:
    from types_boto3_stepfunctions import SFNClient as Boto3SFNClient

    from clearskies_aws.actions import AssumeRole


class StepFunctionsClient(Injectable):
    """
    Injectable for AWS Step Functions client.

    Usage:
        # Use factory defaults
        class MyClass:
            sfn = inject.StepFunctionsClient()

        # Override region for this service
        bindings = {
            "step_functions_client": StepFunctionsClient(region_name="us-east-1")
        }
    """

    def __init__(
        self,
        region_name: str | None = None,
        assume_role: AssumeRole | None = None,
        cache: bool = True,
    ):
        """
        Configure the Step Functions client injectable.

        Args:
            region_name: AWS region (uses factory default if None)
            assume_role: AssumeRole for credential management (uses factory default if None)
            cache: Whether to cache the client (default: True)
        """
        self.region_name = region_name
        self.assume_role = assume_role
        self.cache = cache

    def __get__(self, instance, parent) -> Boto3SFNClient:
        if instance is None:
            return self  # type: ignore

        factory = self._di.build("aws_client_factory", cache=True)

        return factory.create_client(
            service_name="stepfunctions",
            region_name=self.region_name,  # None means use factory default
            assume_role=self.assume_role,  # None means use factory default
            cache=self.cache,
        )
