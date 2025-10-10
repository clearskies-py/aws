from __future__ import annotations

import unittest
from unittest.mock import MagicMock, call

import clearskies
import jinja2
import pytest
from clearskies.di import Di

from clearskies_aws.actions.ses import SES


class SESTest(unittest.TestCase):
    def setUp(self):
        self.di = Di()
        self.di.add_binding("environment", {"AWS_REGION": "us-east-2"})
        self.ses = MagicMock()
        self.ses.send_email = MagicMock()
        self.boto3 = MagicMock()
        self.boto3.client = MagicMock(return_value=self.ses)
        self.environment = MagicMock()
        self.environment.get = MagicMock(return_value="us-east-1")

    def test_send(self):
        # Add utcnow to DI container
        self.di.add_binding("utcnow", MagicMock(return_value=MagicMock()))

        # Create SES with proper parameters - use jinja2.Template object
        ses = SES(
            sender="test@example.com",
            to="jane@example.com",
            subject="welcome!",
            message_template=jinja2.Template("hi {{ model.id }}!"),
        )

        # Manually inject dependencies (bypassing clearskies DI for testing)
        ses.environment = self.environment
        ses.boto3 = self.boto3
        # Don't set ses.di = self.di since the type is incompatible

        # Configure the action
        ses.configure()

        # Mock the DI build method for utcnow
        ses.di = MagicMock()
        ses.di.build.return_value = MagicMock()
        ses.di.call_function = MagicMock()

        # Test the action
        model = MagicMock()
        model.id = "asdf"
        ses(model)
        self.ses.send_email.assert_has_calls(
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
        ses = SES(
            sender="test@example.com",
            to=lambda model: "jane@example.com",
            bcc=lambda model: ["bob@example.com", "greg@example.com"],
            subject="welcome!",
            message_template=jinja2.Template("hi {{ model.id }}!"),
        )

        # Manually inject dependencies (bypassing clearskies DI for testing)
        ses.environment = self.environment
        ses.boto3 = self.boto3

        # Configure the action
        ses.configure()

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

        self.ses.send_email.assert_has_calls(
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
