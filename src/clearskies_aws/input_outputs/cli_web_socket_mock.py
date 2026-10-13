from __future__ import annotations

import json

import clearskies


class CliWebSocketMock(clearskies.input_outputs.Cli):
    """
    Mock an API Gateway websocket request from the command line.

    This behaves like the standard CLI input/output, but requires a `connection_id` in the JSON request body and
    exposes it (along with empty placeholders for the event, context, domain, and stage) via `context_specifics`,
    so that websocket handlers can be exercised locally.
    """

    def context_specifics(self):
        connection_id = json.loads(self.get_body()).get("connection_id")
        if not connection_id:
            raise KeyError("When using the CliWebsocketMock you must provide connection_id in the request body")

        return {
            "event": {},
            "context": {},
            "connection_id": connection_id,
            "domain": "",
            "stage": "",
        }
