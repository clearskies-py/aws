from __future__ import annotations

import json
import unittest

import clearskies

from clearskies_aws.contexts.lambda_sqs_standard import LambdaSqsStandard


class LambdaSqsStandardTest(unittest.TestCase):
    def setUp(self):
        self.calls = []

    def my_callable(self, event, context):
        if "boom" in event:
            raise ValueError("oops")
        self.calls.append(event)

    def test_simple_execution(self):
        records = {
            "Records": [
                {
                    "messageId": "1-2-3-4",
                    "body": json.dumps({"hey": "sup"}),
                },
                {
                    "messageId": "2-3-4-5",
                    "body": json.dumps({"cool": "yo"}),
                },
            ]
        }
        sqs_handler = LambdaSqsStandard(clearskies.endpoints.Callable(self.my_callable))

        sqs_handler(records, {})
        self.assertEqual(
            records,
            self.calls[0],
        )

    def test_with_failure(self):
        records = {
            "Records": [
                {
                    "messageId": "1-2-3-4",
                    "body": json.dumps({"hey": "sup"}),
                },
                {
                    "messageId": "2-3-4-5",
                    "body": json.dumps({"boom": "yo"}),
                },
            ]
        }
        sqs_handler = LambdaSqsStandard(clearskies.endpoints.Callable(self.my_callable))
        (status_code, response_data, response_headers) = sqs_handler(records, {})

        self.assertEqual(
            records,
            self.calls[0],
        )
        self.assertEqual(
            {"batchItemFailures": [{"itemIdentifier": "2-3-4-5"}]},
            response_data,
        )
