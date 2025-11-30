from __future__ import annotations

import json
import unittest

import clearskies
from clearskies import columns, validators

from clearskies_aws.contexts.lambda_sns import LambdaSns

from .my_awesome_model import MyAwesomeModel


class LambdaSnsTest(unittest.TestCase):
    def setUp(self):
        clearskies.backends.MemoryBackend.clear_table_cache()

    def test_invoke(self):
        application = LambdaSns(
            clearskies.endpoints.Create(
                MyAwesomeModel,
                readable_column_names=["id", "name", "email", "created_at"],
                writeable_column_names=["name", "email"],
                url="/create",
            )
        )
        response = application(
            {
                "Records": [
                    {
                        "EventVersion": "1.0",
                        "EventSubscriptionArn": "arn:aws:sns:us-east-1:123456789012:ExampleTopic:uuid",
                        "EventSource": "aws:sns",
                        "Sns": {
                            "SignatureVersion": "1",
                            "Timestamp": "2025-11-28T12:00:00.000Z",
                            "Signature": "signature-string",
                            "SigningCertUrl": "sns.us-east-1.amazonaws.com",
                            "MessageId": "95df01b4-ee98-5cb9-9903-4c221d41eb5e",
                            "Message": json.dumps({"name": "Bob", "email": "bob@example.com"}),
                            "MessageAttributes": {"TestAttribute": {}, "TestBinaryAttribute": {}},
                            "Type": "Notification",
                            "UnsubscribeUrl": "sns.us-east-1.amazonaws.com",
                            "TopicArn": "arn:aws:sns:us-east-1:123456789012:ExampleTopic",
                            "Subject": "An Example Subject",
                        },
                    }
                ]
            },
            {},
            url="create",
            request_method="POST",
        )

        # SNS doesn't have a response, so we have to check the models
        models = application.build(MyAwesomeModel)
        assert len(models) == 1
        assert ["bob@example.com"] == [model.email for model in models]
