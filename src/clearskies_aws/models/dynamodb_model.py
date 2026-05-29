from __future__ import annotations

from typing import Self

import clearskies

import clearskies_aws


class DynamodbModel(clearskies.Model):
    backend = clearskies_aws.backends.DynamodbBackend()

    def query_with_index(self, index_name: str, consistent_read: bool | None = None, check_query=True):
        """
        Use a dynamodb query against a specific index.

        To use an index, you must specifically instruct dynamodb that you are issuing a query operation.
        This method instructs clearskies to execute a query operation against the given index.
        """
        self.no_single_model()
        okay = self.with_query(
            self.get_query().query_with_index(index_name, consistent_read=consistent_read, check_query=check_query)
        )
        return okay

    def get_query(self) -> clearskies.query.Query:
        """Fetch the query object in the model."""
        return self._query if self._query else clearskies_aws.query.DynamodbQuery(self.__class__)

    def sort_by(
        self: Self,
        primary_column_name: str,
        primary_direction: str,
        primary_table_name: str = "",
        secondary_column_name: str = "",
        secondary_direction: str = "",
        secondary_table_name: str = "",
    ) -> Self:
        if secondary_column_name or secondary_direction or secondary_table_name:
            raise ValueError(
                "The Dynamodb model does not support sorting by a second column.  You can only sort by the SORT_KEY in your index."
            )
        if primary_table_name and primary_table_name != self.destination_name():
            raise ValueError(
                f"The Dynamodb model does not support sorting by any table except the main table.  I received a request to sort on table '{primary_table_name}', but my table name is '{self.destination_name()}'"
            )
        return super().sort_by(
            primary_column_name,
            primary_direction,
            "",
        )
