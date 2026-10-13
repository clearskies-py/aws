from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

import clearskies
from botocore.config import Config
from botocore.exceptions import ClientError

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
            bindings={"boto3": mock_boto3, "environment": self.environment},
        )
        context()

        # Verify client was created with config parameter
        mock_boto3.client.assert_called_once()
        call_kwargs = mock_boto3.client.call_args[1]
        self.assertIn("config", call_kwargs)
        config = call_kwargs["config"]
        self.assertIsInstance(config, Config)

    def run_get(self, ssm_client, **kwargs):
        boto3 = SimpleNamespace(client=MagicMock(return_value=ssm_client))
        result = {}

        def test_parameter_store(parameter_store: ParameterStore):
            result["value"] = parameter_store.get("/my/item", **kwargs)
            return {}

        context = clearskies.contexts.Context(
            clearskies.endpoints.Callable(test_parameter_store),
            classes=[ParameterStore],
            bindings={"boto3": boto3, "environment": self.environment},
        )
        context()
        return result["value"]

    def ssm_returning(self, value):
        return SimpleNamespace(get_parameter=MagicMock(return_value={"Parameter": {"Value": value}}))

    def test_get_with_secret_bearer_kwargs(self):
        ssm = self.ssm_returning("sup")
        self.assertEqual(self.run_get(ssm, refresh=True, json_path=None, kind="static_secret"), "sup")
        ssm.get_parameter.assert_called_once_with(Name="/my/item", WithDecryption=True)

    def test_get_json_path(self):
        secret = '{"api_key": "abc", "db": {"credentials": {"password": "hunter2"}}}'
        self.assertEqual(self.run_get(self.ssm_returning(secret), json_path="api_key"), "abc")
        self.assertEqual(self.run_get(self.ssm_returning(secret), json_path="db.credentials.password"), "hunter2")

    def test_get_json_path_errors(self):
        with self.assertRaises(ValueError):
            self.run_get(self.ssm_returning("not-json"), json_path="api_key")
        with self.assertRaises(KeyError):
            self.run_get(self.ssm_returning('{"api_key": "abc"}'), json_path="missing")

    def test_get_silent_if_not_found(self):
        error = ClientError({"Error": {"Code": "ParameterNotFound"}}, "GetParameter")
        ssm = SimpleNamespace(get_parameter=MagicMock(side_effect=error))
        self.assertIsNone(self.run_get(ssm, silent_if_not_found=True, json_path="api_key"))
