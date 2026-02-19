"""AWS client wrappers for clearskies dependency injection."""

from __future__ import annotations

from clearskies_aws.clients.base_aws_client import BaseAwsClient
from clearskies_aws.clients.dynamodb_client import DynamoDbClient
from clearskies_aws.clients.dynamodb_resource import DynamoDbResource
from clearskies_aws.clients.ses_client import SesClient
from clearskies_aws.clients.sns_client import SnsClient
from clearskies_aws.clients.sqs_client import SqsClient
from clearskies_aws.clients.step_functions_client import StepFunctionsClient

__all__ = [
    "BaseAwsClient",
    "DynamoDbClient",
    "DynamoDbResource",
    "SesClient",
    "SnsClient",
    "SqsClient",
    "StepFunctionsClient",
]
