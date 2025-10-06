from __future__ import annotations

from clearskies.contexts import cli

from clearskies_aws.input_outputs import CLIWebSocketMock as CLIWebSocketMockInputOutput


class CLIWebSocketMock(cli.Cli):
    def __call__(self):
        return self.execute_application(CLIWebSocketMockInputOutput())
