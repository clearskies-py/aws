from __future__ import annotations

from clearskies.contexts import cli

from clearskies_aws.input_outputs import CliWebSocketMock as CliWebSocketMockInputOutput


class CliWebSocketMock(cli.Cli):
    """
    Help assist with testing websockets locally.

    The LambdaApiGatewayWebSocket context makes it easy to run websocket applications, but testing
    these locally is literally impossible.  This context provides a close analogue to the way
    the LambdaApiGatewayWebSocket context works to give some testing capabilities when running
    locally.  It works identically to `clearskies.contexts.Cli` but you have to provide a
    `connection_id` property in the JSON body.
    """

    def __call__(self):
        return self.execute_application(CLIWebSocketMockInputOutput())
