from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

import clearskies

from clearskies_aws.secrets.parameter_store import ParameterStore


class ParameterStoreTest(unittest.TestCase):
    def setUp(self):
        parameter_store = SimpleNamespace(get_parameter=MagicMock(return_value={"Parameter": {"Value": "sup"}}))
        self.boto3 = SimpleNamespace(client=MagicMock(return_value=parameter_store))
        self.botocore = SimpleNamespace(client=SimpleNamespace(ClientError=Exception))
        self.environment = SimpleNamespace(get=MagicMock(return_value="us-east-1"))

    def test_get(self):

        def test_parameter_store(parameter_store: ParameterStore):
            parameter_store.get("/my/item")
            return parameter_store

        context = clearskies.contexts.Context(
            clearskies.endpoints.Callable(test_parameter_store),
            classes=[ParameterStore],
            bindings={"boto3": self.boto3, "environment": self.environment},
        )
        (status_code, response_data, response_headers) = context()
        self.assertEqual(status_code, 200)
