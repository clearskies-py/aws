from __future__ import annotations

from typing import TYPE_CHECKING

from clearskies_aws.di.inject.client import Client

if TYPE_CHECKING:
    from types_boto3_dynamodb import DynamoDBClient as Boto3DynamoDBClient

    from clearskies_aws.clients import BaseAwsClient


class DynamodbClient(Client):
    """Injectable for AWS DynamoDB client."""

    @property
    def client_class(self) -> type[BaseAwsClient]:
        from clearskies_aws.clients import DynamodbClient

        return DynamodbClient

    def __get__(self, instance, parent) -> Boto3DynamoDBClient:
        if instance is None:
            return self  # type: ignore

        return self.build_client(instance)  # type: ignore
