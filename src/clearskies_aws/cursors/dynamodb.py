from types import ModuleType

import clearskies.configs
from clearskies.cursors.cursor import Cursor

from types_boto3_dynamodb import DynamoDBClient as Boto3DynamoDBClient

class Dynamodb(Cursor):
    """
    A clearskies PartiQL cursor for DynamoDB.
    """

    dynamodb: Boto3DynamoDBClient
    table_escape_character = '"'
    column_escape_character = '"'
    value_placeholder = "?"

    def __init__(
        self,
        dynamodb: Boto3DynamoDBClient
    ):
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
        print(sql)
        print(parameters)
        try:
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
        """Dynmaodb does not support lastrowid"""
        raise NotImplementedError("Dynamodb doesn't support lastrowid.  Make sure you are using a uuid (or some other auto-generated value) for your model id.")
