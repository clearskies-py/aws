import datetime
from types import ModuleType
from typing import Any

import boto3 as boto3_module
from clearskies import Environment
from clearskies.di import AdditionalConfigAutoImport
from clearskies.di.additional_config import AdditionalConfig

from clearskies_aws.secrets import ParameterStore


class AwsAdditionalConfigAutoImport(AdditionalConfigAutoImport):
    """
    Provide a DI with AWS modules built-in.

    This DI auto injects boto3, boto3 Session and the parameter store.
    """

    def provide_boto3_sdk(self) -> ModuleType:
        import boto3

        return boto3

    def provide_parameter_store(self) -> ParameterStore:
        # This is just here so that we can auto-inject the secrets into the environment without having
        # to force the developer to define a secrets manager
        return ParameterStore()

    def provide_boto3_session(self, boto3: ModuleType, environment: Environment) -> boto3_module.session.Session:
        if not environment.get("AWS_REGION", True):
            raise ValueError(
                "To use AWS Session you must use set AWS_REGION in the .env file or an environment variable"
            )

        session = boto3.session.Session(region_name=environment.get("AWS_REGION", True))
        return session

    def provide_sqs_client(self) -> Any:
        """Provide the SQS client wrapper for dependency injection."""
        from clearskies_aws.clients.sqs_client import SqsClient

        # SqsClient is InjectableProperties, so DI will inject boto3 and environment
        return SqsClient()

    def provide_sns_client(self) -> Any:
        """Provide the SNS client wrapper for dependency injection."""
        from clearskies_aws.clients.sns_client import SnsClient

        # SnsClient is InjectableProperties, so DI will inject boto3 and environment
        return SnsClient()

    def provide_ses_client(self) -> Any:
        """Provide the SES client wrapper for dependency injection."""
        from clearskies_aws.clients.ses_client import SesClient

        return SesClient()

    def provide_step_functions_client(self) -> Any:
        """Provide the Step Functions client wrapper for dependency injection."""
        from clearskies_aws.clients.step_functions_client import StepFunctionsClient

        return StepFunctionsClient()

    def provide_dynamodb_client(self) -> Any:
        """Provide the DynamoDB client wrapper for dependency injection."""
        from clearskies_aws.clients.dynamodb_client import DynamoDbClient

        return DynamoDbClient()

    def provide_dynamodb_resource(self) -> Any:
        """Provide the DynamoDB resource wrapper for dependency injection."""
        from clearskies_aws.clients.dynamodb_resource import DynamoDbResource

        return DynamoDbResource()
