"""Injectable for Step Functions client."""

from __future__ import annotations

from clearskies.di.injectable import Injectable
from types_boto3_stepfunctions import SFNClient as Boto3SFNClient

from clearskies_aws.clients import StepFunctionsClient

class StepFunctionsClient(Injectable):
    """
    Injectable wrapper for Step Functions client.

    This injectable provides access to a boto3 Step Functions client instance that is
    configured through the clearskies DI system.

    Usage::

        from clearskies_aws.di import inject


        class MyAction(InjectableProperties):
            sfn = inject.StepFunctionsClient()

            def start_execution(self):
                self.sfn.start_execution(stateMachineArn="arn:aws:states:...", input='{"key": "value"}')
    """

    client_class = StepFunctionsClient

    def __get__(self, instance, parent) -> Boto3SFNClient:
        """
        Get the Step Functions client from the DI container.

        Returns:
            Boto3 Step Functions client instance
        """
        if parent is None:
            return instance # type: ignore

        return self.build_client() # type: ignore
