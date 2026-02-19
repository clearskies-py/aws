from __future__ import annotations

import json
import unittest
from collections import OrderedDict
from unittest.mock import MagicMock, call

import boto3
import clearskies
import pytest
from clearskies.di import Di

from clearskies_aws.actions.sns import SNS
from clearskies_aws.clients import SnsClient


class MockSnsClient(SnsClient):
    """Mock SnsClient for testing."""

    def __init__(self, mock_boto_client):
        super().__init__()
        self.mock_boto_client = mock_boto_client

    def __call__(self):
        return self.mock_boto_client

    def __getattr__(self, name):
        # Delegate all attribute access to the mock boto client
        return getattr(self.mock_boto_client, name)


class User(clearskies.Model):
    backend = clearskies.backends.MemoryBackend()
    id_column_name = "id"

    id = clearskies.columns.String("id")
    name = clearskies.columns.String("name")
    email = clearskies.columns.Email("email")


class SNSTest(unittest.TestCase):
    def setUp(self):
        self.di = Di()
        self.di.add_binding("environment", {"AWS_REGION": "us-east-2"})
        self.users = self.di.build(User)
        self.sns_boto_client = MagicMock()
        self.sns_boto_client.publish = MagicMock()
        self.when = None

    def always(self, model):
        self.when = model
        return True

    def never(self, model):
        self.when = model
        return False

    def test_send(self):
        mock_client_wrapper = MockSnsClient(self.sns_boto_client)
        sns = SNS(
            topic="arn:aws:my-topic",
            when=self.always,
            client=mock_client_wrapper,
        )

        # Mock the DI for callable resolution
        sns.di = MagicMock()

        def mock_call_function(func, **kwargs):
            return func(kwargs.get("model"))

        sns.di.call_function = mock_call_function

        user = self.users.model(
            {
                "id": "1-2-3-4",
                "name": "Jane",
                "email": "jane@example.com",
            }
        )
        sns(user)
        self.sns_boto_client.publish.assert_has_calls(
            [
                call(
                    TopicArn="arn:aws:my-topic",
                    Message=json.dumps(
                        {
                            "email": "jane@example.com",
                            "id": "1-2-3-4",
                            "name": "Jane",
                        }
                    ),
                ),
            ]
        )
        self.assertEqual(id(user), id(self.when))

    def test_not_now(self):
        mock_client_wrapper = MockSnsClient(self.sns_boto_client)
        sns = SNS(
            topic="arn:aws:my-topic",
            when=self.never,
            client=mock_client_wrapper,
        )

        # Mock the DI for callable resolution
        sns.di = MagicMock()

        def mock_call_function(func, **kwargs):
            return func(kwargs.get("model"))

        sns.di.call_function = mock_call_function

        user = self.users.model(
            {
                "id": "1-2-3-4",
                "name": "Jane",
                "email": "jane@example.com",
            }
        )
        sns(user)
        self.sns_boto_client.publish.assert_not_called()
        self.assertEqual(id(user), id(self.when))
