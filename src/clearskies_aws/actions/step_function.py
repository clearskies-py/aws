from __future__ import annotations

from typing import Callable

import boto3
from clearskies import Model
from clearskies.configs import Callable as CallableConfig
from clearskies.configs import String
from clearskies.decorators import parameters_to_properties
from types_boto3_stepfunctions import SFNClient

from .action_aws import ActionAws
from .assume_role import AssumeRole


class StepFunction(ActionAws[SFNClient]):
    arn = String(required=False)
    arn_environment_key = String(required=False)
    arn_callable = CallableConfig(required=False)
    column_to_store_execution_arn = String(required=False)

    @parameters_to_properties
    def __init__(
        self,
        arn: str | None = None,
        arn_environment_key: str | None = None,
        arn_callable: Callable | None = None,
        column_to_store_execution_arn: str | None = None,
        message_callable: Callable | None = None,
        when: Callable | None = None,
        assume_role: AssumeRole | None = None,
    ) -> None:
        """Configure the Step Function action."""
        super().__init__(
            service_name="stepfunctions", message_callable=message_callable, when=when, assume_role=assume_role
        )

    def configure(self):
        self.finalize_and_validate_configuration()

        arns = 0
        for value in [self.arn, self.arn_environment_key, self.arn_callable]:
            if value:
                arns += 1
        if arns > 1:
            raise ValueError(
                "You can only provide one of 'arn', 'arn_environment_key', or 'arn_callable', but more than one was provided."
            )
        if not arns:
            raise ValueError("You must provide at least one of 'arn', 'arn_environment_key', or 'arn_callable'.")

    def _execute_action(self, client: SFNClient, model: Model) -> None:
        """Send a notification as configured."""
        arn = self.get_arn(model)
        default_region = self.default_region()
        arn_region = arn.split(":")[3]
        if default_region and default_region != arn_region:
            self.region = arn_region
            client = self._get_client()
        response = client.start_execution(
            stateMachineArn=self.get_arn(model),
            input=self.get_message_body(model),
        )

        if self.column_to_store_execution_arn:
            model.save({self.column_to_store_execution_arn: response["executionArn"]})

    def get_arn(self, model: Model) -> str:
        if self.arn:
            return self.arn
        if self.arn_environment_key:
            return self.environment.get(self.arn_environment_key)
        return self.di.call_function(self.arn_callable, model=model)
