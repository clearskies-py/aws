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

    def provide_boto3(self) -> ModuleType:
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
