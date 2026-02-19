from __future__ import annotations

from typing import Any

from clearskies import Model

from clearskies_aws.actions.step_function import StepFunction as BaseStepFunction


class StepFunction(BaseStepFunction):
    calls: list[dict[str, Any]] | None = None

    @classmethod
    def mock(cls, di):
        cls.calls = []
        di.mock_class(BaseStepFunction, StepFunction)

    def __call__(self, model: Model) -> None:
        """Record Step Function start execution call without actually starting."""
        if StepFunction.calls is None:
            StepFunction.calls = []

        StepFunction.calls.append(
            {
                "stateMachineArn": self.get_arn(model),
                "input": self.get_message_body(model),
            }
        )

        if self.column_to_store_execution_arn:
            model.save({self.column_to_store_execution_arn: "mock_execution_arn"})
