"""DynamoDB resource wrapper for clearskies DI."""

from __future__ import annotations

from types_boto3_dynamodb import DynamoDBServiceResource

from clearskies_aws.clients.base_aws_client import BaseAwsClient


class DynamoDbResource(BaseAwsClient):
    """
    High-level DynamoDB table operations.

    Provides a configurable wrapper around boto3's DynamoDB resource for clearskies dependency
    injection. Supports region configuration, role assumption, and resource caching through
    inherited [`BaseAwsClient`](base_aws_client.py) configuration options.

    The DynamoDB resource provides a higher-level, object-oriented interface for working
    with tables, items, and batches. This is the recommended way to interact with DynamoDB
    for most use cases. Use [`DynamoDbClient`](dynamodb_client.py) only when you need
    low-level operations like PartiQL queries or administrative tasks.
    """

    def __call__(self) -> DynamoDBServiceResource:
        """
        Get or create the DynamoDB resource.

        Returns a cached resource if caching is enabled, otherwise creates a new one.

        Example:
            Direct instantiation

            ```python
            from clearskies_aws.clients import DynamoDbResource

            dynamodb = DynamoDbResource(region_name="us-west-2")
            resource = dynamodb()

            table = resource.Table("Users")
            response = table.get_item(Key={"user_id": "user123"})
            print(response["Item"])
            ```

        Example:
            Injectable pattern in a backend

            ```python
            from clearskies.backends import Backend
            from clearskies.di.injectable_properties import InjectableProperties
            from clearskies_aws.di import inject
            from clearskies import Model


            class MyBackend(Backend, InjectableProperties):
                dynamodb = inject.DynamoDbResource()

                def create(self, data: dict, model: Model):
                    table = self.dynamodb().Table(model.table_name())
                    table.put_item(Item=data)
                    return data

                def records(self, configuration: dict, model: Model):
                    table = self.dynamodb().Table(model.table_name())
                    response = table.scan()
                    return response["Items"]
            ```

        Example:
            Batch operations

            ```python
            from clearskies_aws.clients import DynamoDbResource

            dynamodb = DynamoDbResource(region_name="us-east-1")
            resource = dynamodb()
            table = resource.Table("Users")

            # Batch write
            with table.batch_writer() as batch:
                for i in range(100):
                    batch.put_item(Item={"user_id": f"user{i}", "name": f"User {i}", "status": "active"})

            # Batch get
            response = resource.batch_get_item(
                RequestItems={
                    "Users": {"Keys": [{"user_id": "user1"}, {"user_id": "user2"}, {"user_id": "user3"}]}
                }
            )
            items = response["Responses"]["Users"]
            ```

        Example:
            Query and scan operations

            ```python
            from clearskies_aws.clients import DynamoDbResource
            from boto3.dynamodb.conditions import Key, Attr

            dynamodb = DynamoDbResource(region_name="us-west-2")
            resource = dynamodb()
            table = resource.Table("Users")

            # Query with partition key
            response = table.query(KeyConditionExpression=Key("user_id").eq("user123"))

            # Query with sort key range
            response = table.query(
                KeyConditionExpression=Key("user_id").eq("user123")
                & Key("timestamp").between("2024-01-01", "2024-12-31")
            )

            # Scan with filter
            response = table.scan(FilterExpression=Attr("status").eq("active") & Attr("age").gte(18))
            ```
        """
        if self.cache and self.cached_client is not None:
            return self.cached_client  # type: ignore

        resource = self.create_resource("dynamodb")

        if self.cache:
            self.cached_client = resource

        return resource  # type: ignore
