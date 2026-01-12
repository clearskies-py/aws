import unittest
from unittest.mock import MagicMock, patch

from clearskies_aws.cursors.iam.rds_mysql import RdsMysql


class TestRdsMysql(unittest.TestCase):
    def setUp(self):
        self.env = MagicMock()
        self.env.get.side_effect = lambda key, silent=False: {
            "DATABASE_HOST": "test-host",
            "DATABASE_USERNAME": "test-user",
            "DATABASE_NAME": "test-db",
            "DATABASE_PORT": "3306",
            "DATABASE_CERT_PATH": "/path/to/cert",
            "DATABASE_AUTOCOMMIT": True,
            "DATABASE_CONNECT_TIMEOUT": "10",
            "DATABASE_REGION": "eu-west-1",
        }.get(key, None)
        self.boto3 = MagicMock()
        session = MagicMock()
        rds_client = MagicMock()
        rds_client.generate_db_auth_token.return_value = "token"
        session.client.return_value = rds_client
        self.boto3.Session.return_value = session

    @patch("clearskies_aws.cursors.iam.rds_mysql.clearskies")
    def test_build_connection_kwargs(self, clearskies_patch):
        clearskies_patch.di.inject.Environment.return_value = self.env
        import sys

        sys.modules["pymysql"] = MagicMock()
        instance = RdsMysql()
        instance.environment = self.env
        instance.boto3 = self.boto3

        kwargs = instance.build_connection_kwargs()
        assert kwargs["user"] == "test-user"
        assert kwargs["host"] == "test-host"
        assert kwargs["database"] == "test-db"
        assert kwargs["port"] == 3306
        assert kwargs["ssl_ca"] == "/path/to/cert"
        assert kwargs["autocommit"] is True
        assert kwargs["connect_timeout"] == 10
        assert kwargs["password"] == "token"

    @patch("clearskies_aws.cursors.iam.rds_mysql.clearskies")
    def test_missing_region_raises(self, clearskies_patch):
        clearskies_patch.di.inject.Environment.return_value = self.env
        instance = RdsMysql()
        instance.environment = MagicMock()
        instance.environment.get.side_effect = lambda key, silent=False: None
        instance.boto3 = self.boto3

        with self.assertRaises(ValueError):
            instance.build_connection_kwargs()
