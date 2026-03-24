from __future__ import annotations

from typing import TYPE_CHECKING

from clearskies_aws.di.injectable.client import Client

if TYPE_CHECKING:
    from types_boto3_dynamodb import DynamoDBClient as Boto3DynamoDBClient

from clearskies_aws.clients import DynamodbClient

class DynamoDbClient(Client):
    """
    Injectable for AWS DynamoDB client.
    """

    client_class = DynamodbClient

    def __get__(self, instance, parent) -> Boto3DynamoDBClient:
        if parent is None:
            return instance # type: ignore

        return self.build_client(instance) # type: ignore
