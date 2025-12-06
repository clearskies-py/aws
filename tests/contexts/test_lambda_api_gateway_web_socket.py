from __future__ import annotations

import json
import unittest

import clearskies
from clearskies import columns, validators

from clearskies_aws.contexts.lambda_api_gateway_web_socket import LambdaApiGatewayWebSocket

from .my_awesome_model import MyAwesomeModel


class LambdaApiGatewayWebSocketTest(unittest.TestCase):
    def setUp(self):
        clearskies.backends.MemoryBackend.clear_table_cache()

    def test_invoke(self):
        application = LambdaApiGatewayWebSocket(
            clearskies.endpoints.Create(
                MyAwesomeModel,
                readable_column_names=["id", "name", "email", "created_at"],
                writeable_column_names=["name", "email"],
                request_methods=["GET"],
            )
        )
        response = application(
            {
                "body": json.dumps({"name": "Bob", "email": "bob@example.com"}),
                "isBase64Encoded": False,
            },
            {},
        )

        # web socket apps don't actually return a response, so we have to check the memory backend
        models = application.build(MyAwesomeModel)
        assert len(models) == 1
        assert ["bob@example.com"] == [model.email for model in models]
