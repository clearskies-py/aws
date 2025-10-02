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
        self.environment = SimpleNamespace(get=MagicMock(return_value="us-east-1"))

    @pytest.mark.broken
    def test_get(self):

        def get_environment(key):
            if key == "AWS_REGION":
                return "us-east-1"
            raise KeyError("Oops")

        def get_boto3_secrets_manager(key):
            if key == "/my/item":
                return {"SecretString": "sup"}
            raise KeyError("Oops")

        boto3 = SimpleNamespace(client=MagicMock(return_value=get_boto3_secrets_manager))

        def test_secrets_manager(secrets_manager: SecretsManager):
            secrets_manager.get("/my/item")
            return secrets_manager

        context = clearskies.contexts.Context(
            clearskies.endpoints.Callable(test_secrets_manager),
            classes=[SecretsManager],
            bindings={"boto3": boto3, "environment": get_environment},
        )
        (status_code, response_data, response_headers) = context()
