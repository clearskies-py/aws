from __future__ import annotations

import json
import unittest

import clearskies

from clearskies_aws.contexts.lambda_sqs_standard import LambdaSqsStandard

from .my_awesome_model import MyAwesomeModel


def throw(request_data):
    if request_data["name"] == "bob":
        raise ValueError("SUP!")


class LambdaSqsStandardTest(unittest.TestCase):
    def setUp(self):
        clearskies.backends.MemoryBackend.clear_table_cache()

    def test_create(self):
        application = LambdaSqsStandard(
            clearskies.endpoints.Create(
                MyAwesomeModel,
                readable_column_names=["id", "name", "email", "created_at"],
                writeable_column_names=["name", "email"],
                url="/create",
            )
        )

        application(
            {
                "Records": [
                    {
                        "messageId": "1-2-3-4",
                        "body": json.dumps({"name": "Bob", "email": "bob@example.com"}),
                    },
                    {
                        "messageId": "2-3-4-5",
                        "body": json.dumps({"name": "Jane", "email": "jane@example.com"}),
                    },
                ]
            },
            {},
            url="/create",
            request_method="POST",
        )

        # there's no response from SQS, so check the data in the backend
        models = application.build(MyAwesomeModel)
        assert len(models) == 2
        assert ["bob@example.com", "jane@example.com"] == [model.email for model in models]

    def test_with_failure(self):
        records = {
            "Records": [
                {
                    "messageId": "1-2-3-4",
                    "body": json.dumps({"name": "jane"}),
                },
                {
                    "messageId": "2-3-4-5",
                    "body": json.dumps({"name": "bob"}),
                },
            ]
        }
        sqs_handler = LambdaSqsStandard(clearskies.endpoints.Callable(throw))
        response = sqs_handler(records, {})

        print(response)
        self.assertEqual(
            {"batchItemFailures": [{"itemIdentifier": "2-3-4-5"}]},
            response,
        )
