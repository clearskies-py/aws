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


class SqsBackendTest(unittest.TestCase):

    def setUp(self):
        sqsclient = SimpleNamespace(send_message=MagicMock(return_value={"name": "sup"}))
        self.boto3 = SimpleNamespace(client=MagicMock(return_value=sqsclient))
        self.botocore = SimpleNamespace(client=SimpleNamespace(ClientError=Exception))
        self.environment = SimpleNamespace(get=MagicMock(return_value="us-east-1"))

    def test_send_message(self):
        class User(
            clearskies.Model,
        ):
            backend = SqsBackend()

            @classmethod
            def destination_name(cls) -> str:
                return "users"

            id_column_name = "name"

            name = clearskies.columns.String()

        def test_sqs_backend(users: User):
            users.create({"name": "sup"})
            return users

        context = clearskies.contexts.Context(
            clearskies.endpoints.Callable(test_sqs_backend),
            classes=[User],
            bindings={"boto3": self.boto3, "environment": self.environment},
        )
        (status_code, response_data, response_headers) = context()
