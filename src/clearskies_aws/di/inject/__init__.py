from __future__ import annotations

# Legacy (keep for backward compatibility)
from clearskies_aws.di.inject.boto3 import Boto3
from clearskies_aws.di.inject.boto3_session import Boto3Session
from clearskies_aws.di.inject.dynamodb_client import DynamoDbClient
from clearskies_aws.di.inject.dynamodb_resource import DynamoDbResource
from clearskies_aws.di.inject.parameter_store import ParameterStore
from clearskies_aws.di.inject.ses_client import SesClient
from clearskies_aws.di.inject.sns_client import SnsClient

# Service Clients (Recommended)
from clearskies_aws.di.inject.sqs_client import SqsClient
from clearskies_aws.di.inject.sqs_retry import SqsRetry
from clearskies_aws.di.inject.step_functions_client import StepFunctionsClient

__all__ = [
    # Legacy (keep for backward compatibility)
    "Boto3",
    "Boto3Session",
    "ParameterStore",
    # Service Clients
    "SqsClient",
    "SqsRetry",
    "SnsClient",
    "SesClient",
    "StepFunctionsClient",
    "DynamoDbClient",
    "DynamoDbResource",
]
