from __future__ import annotations

import json
import unittest

import clearskies
from clearskies import columns, validators

from clearskies_aws.contexts.lambda_alb import LambdaAlb

from .my_awesome_model import MyAwesomeModel


class LambdaAlbTest(unittest.TestCase):
    def setUp(self):
        clearskies.backends.MemoryBackend.clear_table_cache()
        self.application = LambdaAlb(
            clearskies.endpoints.Create(
                MyAwesomeModel,
                readable_column_names=["id", "name", "email", "created_at"],
                writeable_column_names=["name", "email"],
                url="/model",
            )
        )

    def test_create(self):
        response = self.application(
            {
                "httpMethod": "POST",
                "path": "/model",
                "queryStringParameters": {},
                "headers": {},
                "body": json.dumps(
                    {
                        "name": "Bob",
                        "email": "bob@example.com",
                    }
                ),
                "isBase64Encoded": False,
            },
            {},
        )

        response_data = json.loads(response["body"])["data"]
        assert response["statusCode"] == 200
        assert response_data["name"] == "Bob"
        assert response_data["email"] == "bob@example.com"

    def test_404(self):
        response = self.application(
            {
                "httpMethod": "POST",
                "path": "/wrong-url",
                "queryStringParameters": {},
                "headers": {},
                "body": json.dumps(
                    {
                        "name": "Bob",
                        "email": "bob@example.com",
                    }
                ),
                "isBase64Encoded": False,
            },
            {},
        )
        assert response["statusCode"] == 404

        response = self.application(
            {
                "httpMethod": "GET",
                "path": "/model",
                "queryStringParameters": {},
                "headers": {},
                "body": json.dumps(
                    {
                        "name": "Bob",
                        "email": "bob@example.com",
                    }
                ),
                "isBase64Encoded": False,
            },
            {},
        )
        assert response["statusCode"] == 404
