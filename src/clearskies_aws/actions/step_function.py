from __future__ import annotations

from typing import Callable

from botocore.exceptions import ClientError
from clearskies import Model
from clearskies.configs import Callable as CallableConfig
from clearskies.configs import String
from clearskies.decorators import parameters_to_properties
from types_boto3_stepfunctions import SFNClient

from clearskies_aws import clients, configs

from .action_aws import ActionAws


class StepFunction(ActionAws[SFNClient]):
    """
    Start AWS Step Functions executions as a model action.

    Provides a clearskies action for triggering Step Functions state machine executions. This action
    can be triggered by model events (like `on_change`, `on_create`, etc.) and automatically starts
    an execution with the model data or a custom input. Inherits all configuration from [`ActionAws`](action_aws.py).

    Configure the state machine ARN using a static value, environment variable, or callable. The execution
    input can be customized with `message_callable` or defaults to the model's JSON representation. Optionally,
    store the execution ARN back to a model column for tracking.

    Example:
        Basic usage with static ARN

        ```python
        import clearskies
        from clearskies_aws.actions import StepFunction
        from collections import OrderedDict


        class Order(clearskies.Model):
            def __init__(self, memory_backend, columns):
                super().__init__(memory_backend, columns)

            def columns_configuration(self):
                return OrderedDict(
                    [
                        clearskies.column_types.string(
                            "status",
                            on_change=[
                                StepFunction(arn="arn:aws:states:us-west-2:123:stateMachine:ProcessOrder")
                            ],
                        ),
                    ]
                )
        ```

    Example:
        Storing execution ARN in model

        ```python
        import clearskies
        from clearskies_aws.actions import StepFunction
        from collections import OrderedDict


        class Job(clearskies.Model):
            def __init__(self, memory_backend, columns):
                super().__init__(memory_backend, columns)

            def columns_configuration(self):
                return OrderedDict(
                    [
                        clearskies.column_types.string("status"),
                        clearskies.column_types.string(
                            "execution_arn",
                            on_create=[
                                StepFunction(
                                    arn_environment_key="JOB_PROCESSOR_ARN",
                                    column_to_store_execution_arn="execution_arn",
                                )
                            ],
                        ),
                    ]
                )
        ```

    Example:
        Custom execution input and dynamic ARN

        ```python
        import clearskies
        from clearskies_aws.actions import StepFunction
        from collections import OrderedDict
        import json


        def get_state_machine_arn(model):
            # Different state machine based on order type
            if model.order_type == "express":
                return "arn:aws:states:us-west-2:123:stateMachine:ExpressOrders"
            return "arn:aws:states:us-west-2:123:stateMachine:StandardOrders"


        def format_execution_input(model):
            return json.dumps(
                {
                    "orderId": model.id,
                    "orderType": model.order_type,
                    "priority": model.priority,
                    "items": model.items,
                }
            )


        class Order(clearskies.Model):
            def __init__(self, memory_backend, columns):
                super().__init__(memory_backend, columns)

            def columns_configuration(self):
                return OrderedDict(
                    [
                        clearskies.column_types.string(
                            "status",
                            on_create=[
                                StepFunction(
                                    arn_callable=get_state_machine_arn,
                                    message_callable=format_execution_input,
                                )
                            ],
                        ),
                    ]
                )
        ```
    """

    # Default client for Step Functions service
    client = configs.AwsClient(required=True, default=clients.StepFunctionsClient())

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
        client: clients.StepFunctionsClient | None = None,
    ) -> None:
        """Configure the Step Function action."""
        self.finalize_and_validate_configuration()

    def finalize_and_validate_configuration(self):
        super().finalize_and_validate_configuration()

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

    def __call__(self, model: Model) -> None:
        """Execute Step Function start execution action."""
        # Check conditional execution
        if self.when and not self.di.call_function(self.when, model=model):
            return

        # Get ARN
        arn = self.get_arn(model)

        # Get client and start execution
        try:
            boto3_client = self.client()
            response = boto3_client.start_execution(
                stateMachineArn=arn,
                input=self.get_message_body(model),
            )

            # Store execution ARN if configured
            if self.column_to_store_execution_arn:
                model.save({self.column_to_store_execution_arn: response["executionArn"]})
        except ClientError as e:
            self.logging.exception("Failed to start Step Function execution.")
            raise e

    def get_arn(self, model: Model) -> str:
        if self.arn:
            return self.arn
        if self.arn_environment_key:
            return self.environment.get(self.arn_environment_key)
        return self.di.call_function(self.arn_callable, model=model)
