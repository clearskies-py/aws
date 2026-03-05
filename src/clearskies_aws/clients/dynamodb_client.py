"""DynamoDB client wrapper for clearskies DI."""

from __future__ import annotations

from types_boto3_dynamodb import DynamoDBClient as Boto3DynamoDBClient

from clearskies_aws.clients.base_aws_client import BaseAwsClient


class DynamoDbClient(BaseAwsClient):
    """
    Low-level DynamoDB operations.

    Provides a configurable wrapper around boto3's DynamoDB client for clearskies dependency
    injection. Supports region configuration, role assumption, and client caching through
    inherited [`BaseAwsClient`](base_aws_client.py) configuration options.

    Use this client for low-level operations like batch writes, PartiQL queries, or administrative
    tasks. For most table operations, consider using [`DynamoDbResource`](dynamodb_resource.py)
    instead, which provides a higher-level, object-oriented interface.
    """

    def __call__(self) -> Boto3DynamoDBClient:
        """
        Get or create the DynamoDB client.

        Returns a cached client if caching is enabled, otherwise creates a new one.

        Example:
            Direct instantiation

            ```python
            from clearskies_aws.clients import DynamoDbClient

            dynamodb = DynamoDbClient(region_name="us-west-2")
            client = dynamodb()

            response = client.describe_table(TableName="Users")
            print(f"Table status: {response['Table']['TableStatus']}")
            ```

        Example:
            Injectable pattern with PartiQL

            ```python
            from clearskies.di.injectable_properties import InjectableProperties
            from clearskies_aws.di import inject
            from clearskies import Model


            class MyService(InjectableProperties):
                dynamodb_client = inject.DynamoDbClient()

                def query_users_by_status(self, status: str):
                    response = self.dynamodb_client().execute_statement(
                        Statement="SELECT * FROM Users WHERE status = ?", Parameters=[{"S": status}]
                    )
                    return response["Items"]
            ```

        Example:
            Batch write operations

            ```python
            from clearskies_aws.clients import DynamoDbClient

            dynamodb = DynamoDbClient(region_name="us-east-1")
            client = dynamodb()

            client.batch_write_item(
                RequestItems={
                    "Users": [
                        {
                            "PutRequest": {
                                "Item": {
                                    "user_id": {"S": "user1"},
                                    "name": {"S": "John Doe"},
                                    "email": {"S": "john@example.com"},
                                }
                            }
                        },
                        {
                            "PutRequest": {
                                "Item": {
                                    "user_id": {"S": "user2"},
                                    "name": {"S": "Jane Smith"},
                                    "email": {"S": "jane@example.com"},
                                }
                            }
                        },
                    ]
                }
            )
            ```
        """
        if self.cache and self.cached_client is not None:
            return self.cached_client  # type: ignore

        client = self.create_client("dynamodb")

        if self.cache:
            self.cached_client = client

        return client  # type: ignore
