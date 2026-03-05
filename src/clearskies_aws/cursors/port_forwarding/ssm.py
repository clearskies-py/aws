import time

import clearskies.configs
from clearskies import decorators
from clearskies.cursors.port_forwarding.port_forwarder import PortForwarder

from clearskies_aws.di import inject


class Ssm(PortForwarder):
    """
    Port forwarder using AWS SSM Session Manager.

    This class sets up a local port forwarding tunnel to a remote host using AWS SSM.
    If instance_id is not provided, it will search for a running instance by Name tag.
    """

    """
    The EC2 instance ID to connect to. If not provided, instance_name will be used to look up the instance.
    """
    instance_id = clearskies.configs.String(default=None)

    """
    The Name tag of the EC2 instance to search for if instance_id is not provided.
    """
    instance_name = clearskies.configs.String(default=None)

    """
    The remote port to forward to.
    """
    remote_port = clearskies.configs.Integer()

    """
    The local port to bind for the forwarding tunnel (default: 0, auto-selects a free port).
    """
    local_port = clearskies.configs.Integer(default=0)

    """
    AWS region.
    """
    region = clearskies.configs.String(default=None)

    """
    AWS CLI profile.
    """
    profile = clearskies.configs.String(default=None)

    """
    Boto3 session or client provider
    """
    boto3 = inject.Boto3()

    @decorators.parameters_to_properties
    def __init__(
        self,
        instance_id=None,
        instance_name=None,
        remote_port=None,
        local_port=0,
        region=None,
        profile=None,
    ):
        self._proc = None
        self.finalize_and_validate_configuration()

    def setup(self, original_host: str, original_port: int) -> tuple[str, int]:
        """
        Establish the port forwarding tunnel and return the local endpoint.

        If instance_id is not set, searches for a running instance by Name tag.

        Returns:
            A tuple containing the local host and local port to connect to (e.g., ("localhost", 12345)).
        """
        # Resolve instance_id if needed
        if not self.instance_id and self.instance_name:
            ec2_api = self.boto3.client("ec2", region_name=self.region)
            running_instances = ec2_api.describe_instances(
                Filters=[
                    {"Name": "tag:Name", "Values": [self.instance_name]},
                    {"Name": "instance-state-name", "Values": ["running"]},
                ]
            )
            instance_ids = []
            for reservation in running_instances["Reservations"]:
                for instance in reservation["Instances"]:
                    instance_ids.append(instance["InstanceId"])
            if len(instance_ids) == 0:
                raise ValueError("Failed to launch SSM tunnel! Cannot find bastion!")
            self.instance_id = instance_ids.pop()

        if self.local_port == 0:
            self.local_port = self.pick_free_port("127.0.0.1")

        if self.remote_port is None:
            raise ValueError("remote_port must be set for SSM port forwarding.")

        if self.is_port_open("127.0.0.1", self.local_port):
            return "127.0.0.1", self.local_port

        ssm_cmd = [
            "aws",
            "ssm",
            "start-session",
            "--target",
            self.instance_id,
            "--document-name",
            "AWS-StartPortForwardingSessionToRemoteHost",
            "--parameters",
            f'{{"host":["{original_host}"],"portNumber":["{self.remote_port}"],"localPortNumber":["{self.local_port}"]}}',
        ]
        if self.region:
            ssm_cmd += ["--region", self.region]
        if self.profile:
            ssm_cmd += ["--profile", self.profile]

        self.logger.debug(f"Starting SSM port forwarding session: {' '.join(ssm_cmd)}")

        self._proc = self.subprocess.Popen(ssm_cmd, stdout=self.subprocess.PIPE, stderr=self.subprocess.PIPE)

        start = time.time()
        while True:
            try:
                test_sock = self.socket.socket(self.socket.AF_INET, self.socket.SOCK_STREAM)
                test_sock.settimeout(0.2)
                test_sock.connect(("127.0.0.1", self.local_port))
                test_sock.close()
                break
            except Exception:
                if self._proc is not None and self._proc.poll() is not None:
                    stderr = self._proc.stderr.read().decode() if self._proc.stderr else ""
                    raise RuntimeError(f"SSM process exited unexpectedly. Stderr: {stderr}")
                if time.time() - start > 10:
                    raise TimeoutError(f"Timeout waiting for port {self.local_port} to open")
                time.sleep(0.1)

        return "127.0.0.1", self.local_port

    def teardown(self):
        if self._proc:
            self._proc.terminate()
            self._proc.wait()
            self._proc = None
