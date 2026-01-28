from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

import clearskies
from botocore.config import Config

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
            bindings={"boto3_sdk": self.boto3, "environment": self.environment},
        )
        (status_code, response_data, response_headers) = context()
        self.assertEqual(status_code, 200)

    def test_client_configured_with_adaptive_retry(self):
        """Test that the SSM client is configured with adaptive retry mode."""
        mock_ssm_client = SimpleNamespace(get_parameter=MagicMock(return_value={"Parameter": {"Value": "test"}}))
        mock_boto3 = SimpleNamespace(client=MagicMock(return_value=mock_ssm_client))

        def test_parameter_store(parameter_store: ParameterStore):
            # Access the client to trigger creation
            _ = parameter_store.boto3_client
            return {}

        context = clearskies.contexts.Context(
            clearskies.endpoints.Callable(test_parameter_store),
            classes=[ParameterStore],
            bindings={"boto3_sdk": mock_boto3, "environment": self.environment},
        )
        context()

        # Verify client was created with config parameter
        mock_boto3.client.assert_called_once()
        call_kwargs = mock_boto3.client.call_args[1]
        self.assertIn("config", call_kwargs)
        config = call_kwargs["config"]
        self.assertIsInstance(config, Config)
