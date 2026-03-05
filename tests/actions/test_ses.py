from __future__ import annotations

import unittest
from unittest.mock import MagicMock, call, patch

import clearskies
import jinja2
import pytest
from clearskies.di import Di

from clearskies_aws.actions.ses import SES
from clearskies_aws.clients import SesClient


class MockSesClient(SesClient):
    """Mock SesClient for testing."""

    def __init__(self, mock_boto_client):
        super().__init__()
        self.mock_boto_client = mock_boto_client

    def __call__(self):
        return self.mock_boto_client

    def __getattr__(self, name):
        # Delegate all attribute access to the mock boto client
        return getattr(self.mock_boto_client, name)


class SESTest(unittest.TestCase):
    def setUp(self):
        self.di = Di()
        self.di.add_binding("environment", {"AWS_REGION": "us-east-2"})
        self.ses_boto_client = MagicMock()
        self.ses_boto_client.send_email = MagicMock()

    def test_send(self):
        # Add utcnow to DI container
        self.di.add_binding("utcnow", MagicMock(return_value=MagicMock()))

        # Create SES with proper parameters -use jinja2.Template object
        mock_client_wrapper = MockSesClient(self.ses_boto_client)
        ses = SES(
            sender="test@example.com",
            to="jane@example.com",
            subject="welcome!",
            message_template=jinja2.Template("hi {{ model.id }}!"),
            client=mock_client_wrapper,
        )

        # Mock the DI build method for utcnow
        ses.di = MagicMock()
        ses.di.build.return_value = MagicMock()
        ses.di.call_function = MagicMock()

        # Test the action
        model = MagicMock()
        model.id = "asdf"
        ses(model)
        self.ses_boto_client.send_email.assert_has_calls(
            [
                call(
                    Destination={"ToAddresses": ["jane@example.com"], "CcAddresses": [], "BccAddresses": []},
                    Message={
                        "Body": {
                            "Html": {"Charset": "utf-8", "Data": "hi asdf!"},
                            "Text": {"Charset": "utf-8", "Data": "hi asdf!"},
                        },
                        "Subject": {"Charset": "utf-8", "Data": "welcome!"},
                    },
                    Source="test@example.com",
                ),
            ]
        )

    def test_send_callable(self):
        # Add utcnow to DI container
        self.di.add_binding("utcnow", MagicMock(return_value=MagicMock()))

        # Create SES with callable destinations
        mock_client_wrapper = MockSesClient(self.ses_boto_client)
        ses = SES(
            sender="test@example.com",
            to=lambda model: "jane@example.com",
            bcc=lambda model: ["bob@example.com", "greg@example.com"],
            subject="welcome!",
            message_template=jinja2.Template("hi {{ model.id }}!"),
            client=mock_client_wrapper,
        )

        # Mock the DI for callable resolution
        ses.di = MagicMock()
        ses.di.build.return_value = MagicMock()

        def mock_call_function(func, **kwargs):
            return func(kwargs.get("model"))

        ses.di.call_function = mock_call_function

        # Test the action
        model = MagicMock()
        model.id = "asdf"
        ses(model)

        self.ses_boto_client.send_email.assert_has_calls(
            [
                call(
                    Destination={
                        "ToAddresses": ["jane@example.com"],
                        "CcAddresses": [],
                        "BccAddresses": ["bob@example.com", "greg@example.com"],
                    },
                    Message={
                        "Body": {
                            "Html": {"Charset": "utf-8", "Data": "hi asdf!"},
                            "Text": {"Charset": "utf-8", "Data": "hi asdf!"},
                        },
                        "Subject": {"Charset": "utf-8", "Data": "welcome!"},
                    },
                    Source="test@example.com",
                ),
            ]
        )
