from __future__ import annotations

import json

import clearskies


class CliWebSocketMock(clearskies.input_outputs.Cli):
    def context_specifics(self):
        connection_id = json.loads(self.get_body()).get("connection_id")
        if not connection_id:
            raise KeyError("When using the CliWebsocketMock you must provide connection_id in the request body")

        return {
            "event": {},
            "context": {},
            "connection_id": connection_id,
        }
