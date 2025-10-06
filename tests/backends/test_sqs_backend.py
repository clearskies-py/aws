from __future__ import annotations

import json
import unittest
from collections import OrderedDict
from types import SimpleNamespace
from unittest.mock import MagicMock

import clearskies
import pytest
from clearskies.di import Di

from clearskies_aws.backends.sqs_backend import SqsBackend


class User(
    clearskies.Model,
):
    backend = SqsBackend()

    @classmethod
    def destination_name(cls) -> str:
        return "users"

    id_column_name = "name"

    name = clearskies.columns.string()


class SqsBackendTest(unittest.TestCase):
    def setUp(self):
        self.di = Di()
        self.di.add_binding("environment", {"AWS_REGION": "us-east-2"})
        self.sqs = SimpleNamespace(send_message=MagicMock())
        self.boto3 = SimpleNamespace(client=MagicMock(return_value=self.sqs))
        self.di.add_binding("boto3", self.boto3)

    def test_send(self):
        user = self.di.build(User)
        user.save({"name": "sup"})
        self.boto3.client.assert_called_with("sqs", region_name="us-east-2")
        self.sqs.send_message.assert_called_with(QueueUrl="users", MessageBody=json.dumps({"name": "sup"}))
