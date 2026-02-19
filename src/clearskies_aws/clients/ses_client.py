"""SES client wrapper for clearskies DI."""

from __future__ import annotations

from types_boto3_ses import SESClient as Boto3SESClient

from clearskies_aws.clients.base_aws_client import BaseAwsClient


class SesClient(BaseAwsClient):
    """
    Send emails with Amazon SES.

    Provides a configurable wrapper around boto3's SES client for clearskies dependency
    injection. Supports region configuration, role assumption, and client caching through
    inherited [`BaseAwsClient`](base_aws_client.py) configuration options.
    """

    def __call__(self) -> Boto3SESClient:
        """
        Get or create the SES client.

        Returns a cached client if caching is enabled, otherwise creates a new one.

        Example:
            Direct instantiation

            ```python
            from clearskies_aws.clients import SesClient

            ses = SesClient(region_name="us-west-2")
            client = ses()
            client.send_email(
                Source="sender@example.com",
                Destination={"ToAddresses": ["recipient@example.com"]},
                Message={"Subject": {"Data": "Hello"}, "Body": {"Text": {"Data": "Hello, World!"}}},
            )
            ```

        Example:
            Injectable pattern in an action

            ```python
            from clearskies.di.injectable_properties import InjectableProperties
            from clearskies_aws.di import inject
            from clearskies import Model


            class MyAction(InjectableProperties):
                ses = inject.SesClient()

                def send_welcome_email(self, model: Model):
                    self.ses().send_email(
                        Source="noreply@example.com",
                        Destination={"ToAddresses": [model.email]},
                        Message={
                            "Subject": {"Data": f"Welcome {model.name}!"},
                            "Body": {"Html": {"Data": "<h1>Welcome!</h1>"}, "Text": {"Data": "Welcome!"}},
                        },
                    )
            ```

        Example:
            Sending HTML and text emails

            ```python
            from clearskies_aws.clients import SesClient

            ses = SesClient(region_name="us-east-1")
            client = ses()

            client.send_email(
                Source="notifications@example.com",
                Destination={"ToAddresses": ["user@example.com"], "CcAddresses": ["manager@example.com"]},
                Message={
                    "Subject": {"Charset": "UTF-8", "Data": "Monthly Report"},
                    "Body": {
                        "Html": {
                            "Charset": "UTF-8",
                            "Data": "<html><body><h1>Monthly Report</h1></body></html>",
                        },
                        "Text": {"Charset": "UTF-8", "Data": "Monthly Report - Please see HTML version"},
                    },
                },
            )
            ```
        """
        if self.cache and self.cached_client is not None:
            return self.cached_client  # type: ignore

        client = self.create_client("ses")

        if self.cache:
            self.cached_client = client

        return client  # type: ignore
