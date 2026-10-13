from __future__ import annotations

import json
from typing import Any

from clearskies import Endpoint
from clearskies.configs import AnyDict, String
from clearskies.di.inject import Di
from clearskies.input_outputs import InputOutput


class SimpleBodyRouting(Endpoint):
    """
    Route requests to callables based on a value in the JSON request body.

    This is useful for websocket-style handlers where there is no URL path to route on. The value found under
    `route_key` in the request body is looked up in `routes`, and the matching callable is invoked with the
    parsed body as `request_data`. A missing or unknown route returns a 404.
    """

    di = Di()

    route_key = String(default="route")
    """The key in the request body that contains the name of the route."""

    routes = AnyDict(default={})
    """A dictionary mapping route names to the callables that handle them."""

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
