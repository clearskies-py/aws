from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

import clearskies
import pytest

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
