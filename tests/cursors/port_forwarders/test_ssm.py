import unittest
from unittest.mock import MagicMock, patch

from clearskies_aws.cursors.port_forwarding.ssm import Ssm


class TestSsmPortForwarder(unittest.TestCase):
    def setUp(self):
        self.boto3 = MagicMock()
        self.subprocess = MagicMock()
        self.socket = MagicMock()
        self.ssm_proc = MagicMock()
        self.subprocess.PIPE = "PIPE"
        self.subprocess.Popen.return_value = self.ssm_proc

    def test_setup_with_instance_id(self):
        port_forwarder = Ssm(
            instance_id="i-123456",
            remote_port=3306,
            local_port=12345,
            region="eu-west-1",
            profile="default",
        )
        port_forwarder.subprocess = self.subprocess
        port_forwarder.socket = self.socket

        # Simulate port not open, then open
        self.socket.socket.return_value = MagicMock()
        self.socket.socket.return_value.connect.side_effect = [Exception("not open"), None]
        self.socket.socket.return_value.settimeout = MagicMock()
        self.socket.socket.return_value.close = MagicMock()
        self.ssm_proc.poll.side_effect = [None, None]
        import time as real_time

        with patch("time.time", side_effect=[0, 0.1, 0.2, 0.3, 0.4, 0.5]):
            with patch("time.sleep", return_value=None):
                host, port = port_forwarder.setup("db.internal", 3306)
        assert host == "127.0.0.1"
        assert port == 12345

    def test_setup_with_instance_name(self):
        port_forwarder = Ssm(
            instance_name="bastion",
            remote_port=3306,
            local_port=12345,
            region="eu-west-1",
            profile="default",
        )
        port_forwarder.subprocess = self.subprocess
        port_forwarder.socket = self.socket
        port_forwarder.boto3 = self.boto3

        ec2_client = MagicMock()
        ec2_client.describe_instances.return_value = {"Reservations": [{"Instances": [{"InstanceId": "i-abcdef"}]}]}
        self.boto3.client.return_value = ec2_client

        self.socket.socket.return_value = MagicMock()
        self.socket.socket.return_value.connect.side_effect = [Exception("not open"), None]
        self.socket.socket.return_value.settimeout = MagicMock()
        self.socket.socket.return_value.close = MagicMock()
        self.ssm_proc.poll.side_effect = [None, None]
        import time as real_time

        with patch("time.time", side_effect=[0, 0.1, 0.2, 0.3, 0.4, 0.5]):
            with patch("time.sleep", return_value=None):
                host, port = port_forwarder.setup("db.internal", 3306)
        assert host == "127.0.0.1"
        assert port == 12345
        assert port_forwarder.instance_id == "i-abcdef"

    def test_setup_instance_not_found(self):
        port_forwarder = Ssm(
            instance_name="bastion",
            remote_port=3306,
            local_port=12345,
            region="eu-west-1",
            profile="default",
        )
        port_forwarder.subprocess = self.subprocess
        port_forwarder.socket = self.socket
        port_forwarder.boto3 = self.boto3

        ec2_client = MagicMock()
        ec2_client.describe_instances.return_value = {"Reservations": []}
        self.boto3.client.return_value = ec2_client

        with self.assertRaises(ValueError):
            port_forwarder.setup("db.internal", 3306)

    def test_teardown(self):
        port_forwarder = Ssm(
            instance_id="i-123456",
            remote_port=3306,
            local_port=12345,
            region="eu-west-1",
            profile="default",
        )
        port_forwarder._proc = self.ssm_proc
        port_forwarder.teardown()
        self.ssm_proc.terminate.assert_called_once()
        self.ssm_proc.wait.assert_called_once()
        assert port_forwarder._proc is None
