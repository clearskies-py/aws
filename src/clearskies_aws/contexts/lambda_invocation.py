from __future__ import annotations

from typing import Any

from clearskies.authentication import Public
from clearskies.contexts.context import Context

from clearskies_aws.input_outputs import LambdaInvocation as LambdaInvocationInputOutput


class LambdaInvocation(Context):

    def __call__(self, event: dict[str, Any], context: dict[str, Any]) -> Any:  # type: ignore[override]
        return self.execute_application(
            LambdaInvocationInputOutput(
                event,
                context,
            )
        )
