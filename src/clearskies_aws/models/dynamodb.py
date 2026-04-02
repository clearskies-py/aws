from __future__ import annotations

import json

import clearskies

import clearskies_aws


class Dynamodb(clearskies.Model):
    """
    A model to assist with dynamodb.

    The main thing this model does is add a `query_with_index` and `query_with_primary_index` methods to your model,
    which are what you use to tell clearskies to execute a query.  In the following example, the dynamodb
    table has a primray index on the `id` column and a global secondary index named `category` with a partition key of
    `category` and a sort key of `name`.  We will use clearskies to query this global secondary index:

    ```
    import clearskies
    import clearskies_aws

    class Product(clearskies_aws.models.Dynamodb):
        id_column_name = "id"
        backend = clearskies_aws.backends.DynamodbBackend()

        id = clearskies.columns.Uuid()
        category = clearskies.columns.Select(["Housewares", "Pets", "Clothes", "Outdoors"])
        name = clearskies.columns.String()

    def fetch_records_from_dynamodb(records: Record):
        return records.where("category=Pets").query_with_index("category")

    cli = clearskies.contexts.Cli(fetch_records_from_dynamodb)
    cli()
    ```

    In addition, clearskies assumes that the primary index has a partition key for the id column, so the following
    two operations are equivalent:

    ```
    records.find("id=1-2-3-4")
    records.query_with_primary_index().find("id=1-2-3-4")
    ```

    """

    id_column_name = "connection_id"

    boto3 = clearskies_aws.di.inject.Boto3()
    connection_id = clearskies.columns.String()
    input_output = clearskies.di.inject.InputOutput()

    def send(self, message):
        if not self:
            raise ValueError("Cannot send message to non-existent connection.")
        if not self.connection_id:
            raise ValueError(
                f"Hmmm... I couldn't find the connection id for the {self.__class__.__name__}.  I'm picky about id column names.  Can you please make sure I have a column called connection_id and that it contains the connection id?"
            )

        domain = self.input_output.context_specifics()["domain"]
        stage = self.input_output.context_specifics()["stage"]
        # only include the stage if we're using the default AWS domain - not with a custom domain
        if ".amazonaws.com" in domain:
            endpoint_url = f"https://{domain}/{stage}"
        else:
            endpoint_url = f"https://{domain}"
        api_gateway = self.boto3.client("apigatewaymanagementapi", endpoint_url=endpoint_url)

        bytes_message = json.dumps(message).encode("utf-8")
        try:
            response = api_gateway.post_to_connection(Data=bytes_message, ConnectionId=self.connection_id)
        except api_gateway.exceptions.GoneException:
            self.delete()
        return response
