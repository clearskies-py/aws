from __future__ import annotations

import json
import unittest

import clearskies
from clearskies import columns, validators

from clearskies_aws.contexts.lambda_invoke import LambdaInvoke

from .my_awesome_model import MyAwesomeModel


class LambdaApiGatewayWebSocketTest(unittest.TestCase):
    def setUp(self):
        clearskies.backends.MemoryBackend.clear_table_cache()

    def test_invoke(self):
        application = LambdaInvoke(
            clearskies.endpoints.Create(
                MyAwesomeModel,
                readable_column_names=["id", "name", "email", "created_at"],
                writeable_column_names=["name", "email"],
                url="/create",
            )
        )
        response = application(
            {"name": "Bob", "email": "bob@example.com"},
            {},
            url="create",
            request_method="POST",
        )

        assert response["data"]["name"] == "Bob"
        assert response["data"]["email"] == "bob@example.com"

    def test_invoke_404(self):
        application = LambdaInvoke(
            clearskies.endpoints.Create(
                MyAwesomeModel,
                readable_column_names=["id", "name", "email", "created_at"],
                writeable_column_names=["name", "email"],
                url="/create",
            )
        )
        response = application(
            {"name": "Bob", "email": "bob@example.com"},
            {},
            url="adsfer",
            request_method="POST",
        )

        assert response["status"] == "client_error"
