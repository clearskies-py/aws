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
        self.sns = MagicMock()
        self.sns.publish = MagicMock()
        self.boto3 = MagicMock()
        self.boto3.client = MagicMock(return_value=self.sns)
        self.when = None
        self.environment = MagicMock()
        self.environment.get = MagicMock(return_value="us-east-1")

    def always(self, model):
        self.when = model
        return True

    def never(self, model):
        self.when = model
        return False

    def test_send(self):
        sns = SNS(
            topic="arn:aws:my-topic",
            when=self.always,
        )

        # Manually inject dependencies (bypassing clearskies DI for testing)
        sns.environment = self.environment
        sns.boto3 = self.boto3

        # Configure the action
        sns.configure()

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
        self.sns.publish.assert_has_calls(
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
        sns = SNS(
            topic="arn:aws:my-topic",
            when=self.never,
        )

        # Manually inject dependencies (bypassing clearskies DI for testing)
        sns.environment = self.environment
        sns.boto3 = self.boto3

        # Configure the action
        sns.configure()

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
        self.sns.publish.assert_not_called()
        self.assertEqual(id(user), id(self.when))
