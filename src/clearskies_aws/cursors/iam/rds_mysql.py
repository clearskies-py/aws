"""
RdsMySql: MySQL cursor with AWS RDS IAM authentication.

This class provides a MySQL cursor that uses AWS RDS IAM DB authentication.
It loads connection parameters from environment variables and generates a temporary
IAM authentication token for secure database access.

Configuration fields:
    - boto3: Injected boto3 provider for AWS API access.
    - environment: Injected environment variable provider.
    - hostname_environment_key: Environment variable for DB host (default: "DATABASE_HOST").
    - username_environment_key: Environment variable for DB user (default: "DATABASE_USERNAME").
    - database_environment_key: Environment variable for DB name (default: "DATABASE_NAME").
    - port_environment_key: Environment variable for DB port (default: "DATABASE_PORT").
    - cert_path_environment_key: Environment variable for SSL CA cert (default: "DATABASE_CERT_PATH").
    - autocommit_environment_key: Environment variable for autocommit (default: "DATABASE_AUTOCOMMIT").
    - connect_timeout_environment_key: Environment variable for connect timeout (default: "DATABASE_CONNECT_TIMEOUT").
    - database_region_key: Environment variable for AWS region (default: "DATABASE_REGION").

Example:
    import clearskies_aws.cursors.iam.rds_mysql

    cursor = clearskies_aws.cursors.iam.rds_mysql.RdsMySql()
    cursor.execute("SELECT 1")
"""

import os
from typing import Any

import clearskies
from clearskies import decorators
from clearskies.cursors import Mysql

from clearskies_aws.di import inject


class RdsMysql(Mysql):
    """MySQL cursor with AWS RDS IAM DB authentication."""

    """Injected boto3 provider for AWS API access."""
    boto3 = inject.Boto3()

    """Injected environment variable provider."""
    environment = clearskies.di.inject.Environment()

    """Environment variable for DB host (default: "DATABASE_HOST")."""
    hostname_environment_key = clearskies.configs.String(default="DATABASE_HOST")

    """Environment variable for DB user (default: "DATABASE_USERNAME")."""
    username_environment_key = clearskies.configs.String(default="DATABASE_USERNAME")

    """Environment variable for DB name (default: "DATABASE_NAME")."""
    database_environment_key = clearskies.configs.String(default="DATABASE_NAME")

    """Environment variable for DB port (default: "DATABASE_PORT")."""
    port_environment_key = clearskies.configs.String(default="DATABASE_PORT")

    """Environment variable for SSL CA cert (default: "DATABASE_CERT_PATH")."""
    cert_path_environment_key = clearskies.configs.String(default="DATABASE_CERT_PATH")

    """Environment variable for autocommit (default: "DATABASE_AUTOCOMMIT")."""
    autocommit_environment_key = clearskies.configs.String(default="DATABASE_AUTOCOMMIT")

    """Environment variable for connect timeout (default: "DATABASE_CONNECT_TIMEOUT")."""
    connect_timeout_environment_key = clearskies.configs.String(default="DATABASE_CONNECT_TIMEOUT")

    """Environment variable for AWS region (default: "DATABASE_REGION")."""
    database_region_key = clearskies.configs.String(default="DATABASE_REGION")

    @decorators.parameters_to_properties
    def __init__(
        self,
        hostname_environment_key: str | None = None,
        username_environment_key: str | None = None,
        database_environment_key: str | None = None,
        port_environment_key: str | None = None,
        cert_path_environment_key: str | None = None,
        autocommit_environment_key: str | None = None,
        database_region_key: str | None = None,
        connect_timeout_environment_key: str | None = None,
        port_forwarding: Any | None = None,
    ):
        self.finalize_and_validate_configuration()

    def build_connection_kwargs(self) -> dict:
        """
        Build the connection kwargs for the MySQL client, using IAM DB authentication.

        Returns
        -------
        dict
            Connection parameters for the MySQL client.
        """
        connection_kwargs = {
            "user": self.environment.get(self.username_environment_key),
            "host": self.environment.get(self.hostname_environment_key),
            "database": self.environment.get(self.database_environment_key),
            "port": int(self.environment.get(self.port_environment_key, silent=True) or self.port),
            "ssl_ca": self.environment.get(self.cert_path_environment_key, silent=True),
            "autocommit": self.environment.get(self.autocommit_environment_key, silent=True),
            "connect_timeout": int(
                self.environment.get(self.connect_timeout_environment_key, silent=True) or self.connect_timeout
            ),
        }
        region: str = self.environment.get(self.database_region_key, True) or self.environment.get("AWS_REGION", True)
        if not region:
            raise ValueError(
                "To use RDS IAM DB auth you must set DATABASE_REGION or AWS_REGION in the .env file or an environment variable"
            )
        os.environ["LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN"] = "1"

        rds_api = self.boto3.Session().client("rds")
        rds_token = rds_api.generate_db_auth_token(
            DBHostname=connection_kwargs.get("host"),
            Port=connection_kwargs.get("port", 3306),
            DBUsername=connection_kwargs.get("user"),
            Region=region,
        )
        connection_kwargs["password"] = rds_token

        for kwarg in ["autocommit", "connect_timeout", "port", "ssl_ca"]:
            if not connection_kwargs[kwarg]:
                del connection_kwargs[kwarg]

        return {**super().build_connection_kwargs(), **connection_kwargs}
