from __future__ import annotations

from clearskies.contexts.context import Context

from ..input_outputs import (
    LambdaAPIGatewayWebSocket as LambdaAPIGatewayWebSocketInputOutput,
)


class LambdaAPIGatewayWebSocket(Context):

    def __call__(self, event, context):
        if self.execute_application is None:
            raise ValueError("Cannot execute LambdaAPIGatewayWebSocket context without first configuring it")

        return self.execute_application(LambdaAPIGatewayWebSocketInputOutput(event, context))
