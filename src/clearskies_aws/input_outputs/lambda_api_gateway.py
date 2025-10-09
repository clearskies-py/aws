from __future__ import annotations

import base64
import json
from typing import Any, Literal, cast
from urllib.parse import urlencode

from aws_lambda_powertools.utilities.parser import parse
from aws_lambda_powertools.utilities.parser.models import (
    APIGatewayProxyEventModel,
)
from aws_lambda_powertools.utilities.typing import LambdaContext
from clearskies.input_outputs.input_output import InputOutput
from clearskies.input_outputs.headers import Headers
from pydantic import ValidationError
from pydantic.networks import IPvAnyNetwork


class LambdaAPIGateway(InputOutput):
    event: APIGatewayProxyEventModel
    context: LambdaContext
    resource = configs.String()
    path = configs.String()
    _cached_body = None
    _body_was_cached = False

    def __init__(self, event: dict, context: LambdaContext):
        try:
            # Manually parse the incoming event into MyEvent model
            self.event = parse(model=APIGatewayProxyEventModel, event=event)
        except ValidationError as e:
            # Catch validation errors and return a 400 response
            raise ValueError(f"Failed to parse event from ApiGateway: {e}")
        self.context = context
        if self.event.version == "1.0":
            self.parse_event_v1()
        elif self.event.version == "2.0":
            self.parse_event_v2()
        else:
            raise ValueError(f"Unsupported API Gateway event version: {self._event.version}")

        super().__init__()

    def parse_event_v1(self):
        self.request_method = self.event.httpMethod.upper()
        self.path = self.event.path
        self.resource = self.event.resource
        self.query_parameters = {
            **(self.event.queryStringParameters or {}),
            **(self.event.multiValueQueryStringParameters or {}),
        }
        self.path_parameters = self.event.pathParameters if self.event.pathParameters else {}
        request_headers = {}
        for key, value in {
            **self.event.headers,
            **self.event.multiValueHeaders,
        }.items():
            request_headers[key.lower()] = str(value)
        self.request_headers = Headers(request_headers)

    def respond(self, body: Any, status_code: int = 200) -> dict[str, Any]:
        if "content-type" not in self.response_headers:
            self.response_headers.content_type = "application/json; charset=UTF-8"

        is_base64 = False

        if isinstance(body, bytes):
            is_base64 = True
            final_body = base64.encodebytes(body).decode("utf8")
        elif isinstance(body, str):
            final_body = body
        else:
            final_body = json.dumps(body)

        return {
            "isBase64Encoded": is_base64,
            "statusCode": status_code,
            "headers": self.response_headers,
            "body": final_body,
        }

    def has_body(self) -> bool:
        return bool(self.get_body())

    def get_body(self) -> Any:
        if not self._body_was_cached:
            self._cached_body = self._event.body
            self._body_was_cached = True
            if self._cached_body is not None and self._event.isBase64Encoded and isinstance(self._cached_body, str):
                self._cached_body = base64.decodebytes(self._cached_body.encode("utf-8")).decode("utf-8")
        return self._cached_body

    def get_protocol(self) -> str:
        return "https"

    def get_full_path(self):
        return self.path

    def context_specifics(self) -> dict[str, Any]:
        return {
            "event": self.event,
            "context": self.context,
            "resource": self.resource,
        }

    def get_client_ip(self) -> IPvAnyNetwork:
        # I haven't actually tested with an API gateway yet to figure out which of these works...
        if hasattr(self._event, "requestContext") and hasattr(self._event.requestContext, "identity"):
            if hasattr(self._event.requestContext.identity, "sourceIp"):
                return cast(IPvAnyNetwork, self._event.requestContext.identity.sourceIp)

        return cast(IPvAnyNetwork, self.get_request_header("x-forwarded-for", silent=True))
