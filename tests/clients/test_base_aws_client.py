"""Regression tests for BaseAwsClient / service client wrappers.

1. `BaseAwsClient.boto3` must resolve to the real boto3 module, not a bare instance of a
   same-named DI-injectable class, when an app registers `modules=[clearskies_aws, ...]`.

2. Every service client wrapper must forward its configured region to `create_client()`, not
   its own service name.
"""

from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock

from clearskies.di import Di

import clearskies_aws
from clearskies_aws.clients import (
    DynamodbClient,
    SesClient,
    SnsClient,
    SqsClient,
    StepFunctionsClient,
)


class Boto3DiNameCollisionTest(unittest.TestCase):
    """Reproduces the exact failure mode from the bug report: modules=[clearskies_aws, ...]."""

    def setUp(self):
        self.di = Di(modules=[clearskies_aws])

    def test_boto3_property_resolves_to_real_module_not_bare_class_instance(self):
        sqs_client = self.di.build(SqsClient)

        boto3_module = sqs_client.boto3

        self.assertTrue(
            hasattr(boto3_module, "client"),
            "Expected the real boto3 module (with a .client() method), got "
            f"{boto3_module!r} instead. This means the 'boto3' DI name is being shadowed by "
            "an auto-registered class again.",
        )

    def test_create_client_succeeds_when_modules_are_registered(self):
        sqs_client = self.di.build(SqsClient)
        sqs_client.aws_region = "us-east-1"

        # Before the fix, this raised: AttributeError: 'Boto3' object has no attribute 'client'
        boto3_sqs_client = sqs_client.create_client()

        self.assertEqual("SQS", type(boto3_sqs_client).__name__)


class ClientWrapperRegionRegressionTest(unittest.TestCase):
    """Every service client wrapper must forward the configured region, not its own service name."""

    def setUp(self):
        self.captured: dict = {}

        outer_self = self

        class FakeBoto3:
            def client(self, service_name, **kwargs):
                outer_self.captured["service_name"] = service_name
                outer_self.captured["kwargs"] = kwargs
                return MagicMock()

        self.fake_boto3 = FakeBoto3()
        # Silent environment mock: aws_region set explicitly per-test takes priority over this,
        # so its return value doesn't matter here as long as it doesn't raise.
        self.environment = SimpleNamespace(get=MagicMock(return_value=None))

    def _build(self, client_class, region):
        # Bind "boto3" (not "boto3"): BaseAwsClient.boto3 = ByStandardLib("boto3") resolves
        # via build_standard_lib("boto3"), which checks build_from_name("boto3") - i.e. explicit
        # bindings under "boto3" - before falling back to a real `import boto3`.
        di = Di(
            classes=[client_class],
            bindings={"boto3": self.fake_boto3, "environment": self.environment},
        )
        client = di.build(client_class)
        client.aws_region = region
        return client

    def test_sqs_client_passes_configured_region_not_service_name(self):
        client = self._build(SqsClient, "us-west-2")
        client()
        self.assertEqual("sqs", self.captured["service_name"])
        self.assertEqual("us-west-2", self.captured["kwargs"].get("region_name"))

    def test_sns_client_passes_configured_region_not_service_name(self):
        client = self._build(SnsClient, "us-west-2")
        client()
        self.assertEqual("sns", self.captured["service_name"])
        self.assertEqual("us-west-2", self.captured["kwargs"].get("region_name"))

    def test_ses_client_passes_configured_region_not_service_name(self):
        client = self._build(SesClient, "us-west-2")
        client()
        self.assertEqual("ses", self.captured["service_name"])
        self.assertEqual("us-west-2", self.captured["kwargs"].get("region_name"))

    def test_dynamodb_client_passes_configured_region_not_service_name(self):
        client = self._build(DynamodbClient, "us-west-2")
        client()
        self.assertEqual("dynamodb", self.captured["service_name"])
        self.assertEqual("us-west-2", self.captured["kwargs"].get("region_name"))

    def test_step_functions_client_passes_configured_region_not_service_name(self):
        client = self._build(StepFunctionsClient, "us-west-2")
        client()
        self.assertEqual("stepfunctions", self.captured["service_name"])
        self.assertEqual("us-west-2", self.captured["kwargs"].get("region_name"))


if __name__ == "__main__":
    unittest.main()
