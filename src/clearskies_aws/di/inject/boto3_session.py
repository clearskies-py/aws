from __future__ import annotations

from types import ModuleType

from clearskies.di import Injectable, inject

from clearskies_aws.di.inject import boto3 as boto3_inject


class Boto3Session(Injectable):
    environment = inject.Environment()
    boto3 = boto3_inject.Boto3()

    def __init__(self, cache: bool = True):
        self.cache = cache

    def __get__(self, instance, parent) -> ModuleType:
        if instance is None:
            return self  # type: ignore
        if not self.environment.get("AWS_REGION", True):
            raise ValueError(
                "To use AWS Session you must use set AWS_REGION in the .env file or an environment variable"
            )

        session = self.boto3.session.Session(region_name=self.environment.get("AWS_REGION", True))
        return session
