import json
from typing import Any, Iterator

import clearskies.configs
from clearskies.cursors.cursor import Cursor
from types_boto3_dynamodb import DynamoDBClient as Boto3DynamoDBClient


class Dynamodb(Cursor):
    """A clearskies PartiQL cursor for DynamoDB."""

    dynamodb: Boto3DynamoDBClient
    table_escape_character = '"'
    column_escape_character = '"'
    value_placeholder = "?"

    next_token: str = ""
    records: list[dict[str, Any]] = []

    def __init__(self, dynamodb: Boto3DynamoDBClient):
        self.dynamodb = dynamodb

    @property
    def cursor(self):
        return self

    def close(self) -> None:
        """Dynamodb doesn't actually have a cursor or connection to close."""
        return

    def execute(self, sql: str, parameters: tuple | list = ()):
        """
        Execute a SQL statement with parameters.

        Args:
            sql: SQL statement to execute.
            parameters: Parameters for the SQL statement.

        Returns
        -------
            Result of cursor.execute().
        """
        try:
            self.records = []
            self.next_token = ""
            self.logger.debug(f"Executing SQL: {sql} with parameters: {parameters}")
            return self.execute_partiql(sql, parameters)
        except Exception:
            self.logger.exception(f"Error executing Partiql: {sql} with parameters: {parameters}")
            raise

    @property
    def connection(self):
        raise NotImplementedError("Dynamodb cursors don't have connection objects")

    @property
    def lastrowid(self) -> int | None:
        """Dynamodb does not support lastrowid."""
        raise NotImplementedError(
            "Dynamodb doesn't support lastrowid.  Make sure you are using a uuid (or some other auto-generated value) for your model id."
        )

    def execute_partiql(self, sql: str, parameters: tuple | list = ()):
        kwargs: dict[str, Any] = {
            "Statement": sql,
            "Parameters": [],
            "ReturnConsumedCapacity": "INDEXES",
        }
        key_to_kwarg_map = {
            "LIMIT": "Limit",
            "CONSISTENT_READ": "ConsistentRead",
            "NEXT_TOKEN": "NextToken",
        }
        for parameter in parameters:
            # our parameters may already be dynamodb friendly (it wants a dictionary with a single entry, the
            # key representing a type) or it may not be.  If not, we need to take the opportunity to convert it.
            if not isinstance(parameter, dict):
                parameter = self.as_partiql_parameter(parameter)

            first_key = list(parameter.keys())[0]
            if first_key in key_to_kwarg_map:
                kwargs[key_to_kwarg_map[first_key]] = parameter[first_key]
            else:
                kwargs["Parameters"].append(parameter)

        if not kwargs["Parameters"]:
            del kwargs["Parameters"]

        result = self.dynamodb.execute_statement(**kwargs)
        self.logger.debug(f"Consumed capacity for query {sql}:")
        self.logger.debug(json.dumps(result["ConsumedCapacity"], indent=2))

        self.records = []
        for record in result["Items"]:
            mapped: dict[str, Any] = {}
            for name, typed_value in record.items():
                first_key = list(typed_value.keys())[0]
                if first_key == "N":
                    mapped[name] = int(typed_value[first_key])
                elif first_key == "NULL":
                    if typed_value[first_key]:
                        mapped[name] = None
                else:
                    mapped[name] = typed_value[first_key]
            self.records.append(mapped)
        self.next_token = result.get("NextToken", "")

    def __iter__(self):
        return iter([*self.records])

    def as_partiql_parameter(self, value: Any) -> dict[str, Any]:
        if isinstance(value, int) or isinstance(value, float):
            return {"N": str(value)}
        if isinstance(value, bool):
            return {"BOOL": value}
        return {"S": str(value)}
