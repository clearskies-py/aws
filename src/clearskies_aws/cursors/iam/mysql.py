import os

import clearskies
from clearskies import decorators
from clearskies.cursors import Mysql as MysqlBase

from clearskies_aws.di import inject


class MySql(MysqlBase):
    """RDS IAM DB auth."""

    boto3 = inject.Boto3()
    environment = clearskies.di.inject.Environment()

    hostname_environment_key = clearskies.configs.String(default="DATABASE_HOST")
    username_environment_key = clearskies.configs.String(default="DATABASE_USERNAME")
    database_environment_key = clearskies.configs.String(default="DATABASE_NAME")

    port_environment_key = clearskies.configs.String(default="DATABASE_PORT")
    cert_path_environment_key = clearskies.configs.String(default="DATABASE_CERT_PATH")
    autocommit_environment_key = clearskies.configs.String(default="DATABASE_AUTOCOMMIT")
    connect_timeout_environment_key = clearskies.configs.String(default="DATABASE_CONNECT_TIMEOUT")

    database_region_key = clearskies.configs.String(default="DATABASE_REGION")
    bastion_instance_name_environment_key = clearskies.configs.String(default="BASTION_INSTANCE_NAME")
    proxy_host_environment_key = clearskies.configs.String(default="DATABASE_PROXY_HOST")
    proxy_port_environment_key = clearskies.configs.String(default="DATABASE_PROXY_PORT")

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
        bastion_instance_name_environment_key: str | None = None,
        proxy_host_environment_key: str | None = None,
        proxy_port_environment_key: str | None = None,
        port_forwarding: bool | None = None,
    ):
        self.finalize_and_validate_configuration()

    def build_connection_kwargs(self) -> dict:
        connection_kwargs = {
            "user": self.environment.get(self.username_environment_key),
            "host": self.environment.get(self.hostname_environment_key),
            "database": self.environment.get(self.database_environment_key),
            "port": int(self.environment.get(self.port_environment_key, silent=True)),
            "ssl_ca": self.environment.get(self.cert_path_environment_key, silent=True),
            "autocommit": self.environment.get(self.autocommit_environment_key, silent=True),
            "connect_timeout": int(self.environment.get(self.connect_timeout_environment_key, silent=True)),
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

        if self.port_forwarding:
            connection_kwargs["host"] = self.environment.get(self.proxy_host_environment_key, True) or "localhost"
            connection_kwargs["port"] = int(self.environment.get(self.proxy_port_environment_key, True) or 3307)

        for kwarg in ["autocommit", "connect_timeout", "port", "ssl_ca"]:
            if not connection_kwargs[kwarg]:
                del connection_kwargs[kwarg]
        build_kwargs = {**super().build_connection_kwargs(), **connection_kwargs}
        return build_kwargs

    def port_forwarding_context(self) -> bool:
        self.logger.info("Attempting to connect via bastion...")
        import time

        endpoint = self.environment.get(self.hostname_environment_key)
        instance_name = self.environment.get(self.bastion_instance_name_environment_key)

        port = int(self.environment.get(self.port_environment_key, silent=True)) or 3306

        proxy_port = int(self.environment.get(self.proxy_port_environment_key, True)) or 3307
        proxy_host = self.environment.get(self.proxy_host_environment_key, True) or "localhost"

        region: str = self.environment.get("DATABASE_REGION", True) or self.environment.get("AWS_REGION", True)
        if not region:
            raise ValueError(
                "To use RDS IAM DB auth you must set DATABASE_REGION or AWS_REGION in the .env file or an environment variable"
            )

        sock = self.socket.socket(self.socket.AF_INET, self.socket.SOCK_STREAM)
        result = sock.connect_ex((proxy_host, proxy_port))
        if result == 0:
            sock.close()
            self.logger.warning("Socket already opened")
            return True

        ec2_api = self.boto3.client("ec2", region_name=region)
        running_instances = ec2_api.describe_instances(
            Filters=[
                {
                    "Name": "tag:Name",
                    "Values": [
                        instance_name,
                    ],
                },
                {"Name": "instance-state-name", "Values": ["running"]},
            ],
        )
        instance_ids = []
        for reservation in running_instances["Reservations"]:
            for instance in reservation["Instances"]:
                instance_ids.append(instance["InstanceId"])

        if len(instance_ids) == 0:
            raise ValueError("Failed to launch SSM tunnel! Cannot find bastion!")  # noqa: TRY003

        instance_id = instance_ids.pop()

        tunnel_command = [
            "aws",
            "--region",
            region,
            "ssm",
            "start-session",
            "--target",
            f"{instance_id}",
            "--document-name",
            "AWS-StartPortForwardingSessionToRemoteHost",
            f'--parameters={{"host":["{endpoint}"], "portNumber":["{port}"],"localPortNumber":["{proxy_port}"]}}',
        ]

        self.logger.debug("Launching SSM tunnel with:")
        self.logger.debug(" ".join(tunnel_command))
        self.subprocess.Popen(tunnel_command)
        connected = False
        attempts = 0
        while not connected and attempts < 6:
            attempts += 1
            time.sleep(0.5)
            sock = self.socket.socket(self.socket.AF_INET, self.socket.SOCK_STREAM)
            result = sock.connect_ex((proxy_host, proxy_port))
            if result == 0:
                connected = True
        if connected:
            self.logger.debug("Connected!")
            return True
        self.logger.error("Failed to launch SSM tunnel!  Try launching manually with the above command")
        return False
