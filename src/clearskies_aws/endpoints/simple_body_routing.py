from __future__ import annotations

import json
from typing import Any

from clearskies import Endpoint
from clearskies.configs import AnyDict, String
from clearskies.di.inject import Di
from clearskies.input_outputs import InputOutput


class SimpleBodyRouting(Endpoint):
    di = Di()

    route_key = String(default="route")
    routes = AnyDict(default={})

    def __init__(self, routes: dict[str, Any], route_key: str = "route"):
        self.routes = routes
        self.route_key = route_key

    def handle(self, input_output: InputOutput) -> Any:
        body = json.loads(input_output.get_body()) if input_output.has_body() else {}

        if not body or not body.get(self.route_key):
            return self.error(input_output, "Not Found", 404)

        route = body[self.route_key]
        if route not in self.routes:
            return self.error(input_output, "Not Found", 404)
        return input_output.respond(
            self.di.call_function(
                self.routes[route],
                request_data=body,
                **input_output.context_specifics(),
            ),
            200,
        )

    def documentation(self):
        return []
