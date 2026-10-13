from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

import clearskies
from botocore.exceptions import ClientError

from clearskies_aws.secrets.secrets_manager import SecretsManager


class SecretsManagerTest(unittest.TestCase):
    def setUp(self):
        secretsmanager = SimpleNamespace(get_secret_value=MagicMock(return_value={"SecretString": "sup"}))
        self.boto3 = SimpleNamespace(client=MagicMock(return_value=secretsmanager))
        self.botocore = SimpleNamespace(client=SimpleNamespace(ClientError=Exception))
        self.environment = SimpleNamespace(get=MagicMock(return_value="us-east-1"))

    def test_get(self):
        def test_secrets_manager(secrets_manager: SecretsManager):
            secrets_manager.get("/my/item")
            return secrets_manager

        context = clearskies.contexts.Context(
            clearskies.endpoints.Callable(test_secrets_manager),
            classes=[SecretsManager],
            bindings={"boto3": self.boto3, "environment": self.environment},
        )
        (status_code, response_data, response_headers) = context()

    def run_get(self, secret_string, **kwargs):
        secretsmanager = SimpleNamespace(get_secret_value=MagicMock(return_value={"SecretString": secret_string}))
        boto3 = SimpleNamespace(client=MagicMock(return_value=secretsmanager))
        result = {}

        def test_secrets_manager(secrets_manager: SecretsManager):
            result["value"] = secrets_manager.get("/my/item", **kwargs)
            return {}

        context = clearskies.contexts.Context(
            clearskies.endpoints.Callable(test_secrets_manager),
            classes=[SecretsManager],
            bindings={"boto3": boto3, "environment": self.environment},
        )
        context()
        return result["value"], secretsmanager.get_secret_value

    def test_get_with_secret_bearer_kwargs(self):
        value, get_secret_value = self.run_get("sup", refresh=True, json_path=None, kind="static_secret")
        self.assertEqual(value, "sup")
        get_secret_value.assert_called_once_with(SecretId="/my/item")

    def test_get_json_path(self):
        secret = '{"api_key": "abc", "db": {"credentials": {"password": "hunter2"}}}'
        self.assertEqual(self.run_get(secret, json_path="api_key")[0], "abc")
        self.assertEqual(self.run_get(secret, json_path="db.credentials.password")[0], "hunter2")

    def test_get_json_path_errors(self):
        with self.assertRaises(ValueError):
            self.run_get("not-json", json_path="api_key")
        with self.assertRaises(KeyError):
            self.run_get('{"api_key": "abc"}', json_path="missing")

    def test_get_silent_if_not_found(self):
        error = ClientError({"Error": {"Code": "ResourceNotFoundException"}}, "GetSecretValue")
        secretsmanager = SimpleNamespace(get_secret_value=MagicMock(side_effect=error))
        boto3 = SimpleNamespace(client=MagicMock(return_value=secretsmanager))
        result = {}

        def test_secrets_manager(secrets_manager: SecretsManager):
            result["value"] = secrets_manager.get("/my/item", silent_if_not_found=True, json_path="api_key")
            return {}

        context = clearskies.contexts.Context(
            clearskies.endpoints.Callable(test_secrets_manager),
            classes=[SecretsManager],
            bindings={"boto3": boto3, "environment": self.environment},
        )
        context()
        self.assertIsNone(result["value"])
