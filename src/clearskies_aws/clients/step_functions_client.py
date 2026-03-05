"""Step Functions client wrapper for clearskies DI."""

from __future__ import annotations

from types_boto3_stepfunctions import SFNClient as Boto3SFNClient

from clearskies_aws.clients.base_aws_client import BaseAwsClient


class StepFunctionsClient(BaseAwsClient):
    """
    Execute AWS Step Functions state machines.

    Provides a configurable wrapper around boto3's Step Functions client for clearskies dependency
    injection. Supports region configuration, role assumption, and client caching through
    inherited [`BaseAwsClient`](base_aws_client.py) configuration options.
    """

    def __call__(self) -> Boto3SFNClient:
        """
        Get or create the Step Functions client.

        Returns a cached client if caching is enabled, otherwise creates a new one.

        Example:
            Direct instantiation

            ```python
            from clearskies_aws.clients import StepFunctionsClient
            import json

            sfn = StepFunctionsClient(region_name="us-west-2")
            client = sfn()
            client.start_execution(
                stateMachineArn="arn:aws:states:us-west-2:123456789012:stateMachine:MyStateMachine",
                input=json.dumps({"key": "value"}),
            )
            ```

        Example:
            Injectable pattern in an action

            ```python
            from clearskies.di.injectable_properties import InjectableProperties
            from clearskies_aws.di import inject
            from clearskies import Model
            import json


            class MyAction(InjectableProperties):
                sfn = inject.StepFunctionsClient()

                def trigger_workflow(self, model: Model):
                    response = self.sfn().start_execution(
                        stateMachineArn="arn:aws:states:us-west-2:123456789012:stateMachine:ProcessUser",
                        name=f"user-{model.id}-{int(time.time())}",
                        input=json.dumps({"user_id": model.id, "action": "process"}),
                    )
                    return response["executionArn"]
            ```

        Example:
            Starting execution with name and tags

            ```python
            from clearskies_aws.clients import StepFunctionsClient
            import json

            sfn = StepFunctionsClient(region_name="us-east-1")
            client = sfn()

            response = client.start_execution(
                stateMachineArn="arn:aws:states:us-east-1:123456789012:stateMachine:DataPipeline",
                name="daily-pipeline-2024-01-15",
                input=json.dumps(
                    {"source": "s3://my-bucket/data.csv", "destination": "s3://my-bucket/processed/"}
                ),
            )

            execution_arn = response["executionArn"]
            print(f"Started execution: {execution_arn}")
            ```
        """
        if self.cache and self.cached_client is not None:
            return self.cached_client  # type: ignore

        client = self.create_client("stepfunctions")

        if self.cache:
            self.cached_client = client

        return client  # type: ignore
