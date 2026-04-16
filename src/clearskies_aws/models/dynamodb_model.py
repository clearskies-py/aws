from __future__ import annotations

from typing import Self

import clearskies

import clearskies_aws


class DynamodbModel(clearskies.Model):
    backend = clearskies_aws.backends.DynamodbBackend()

    def query_with_index(self, index_name: str, consistent_read: bool | None=None, check_query=True):
        """
        Use a dynamodb query against a specific index.

        To use an index, you must specifically instruct dynamodb that you are issuing a query operation.
        This method instructs clearskies to execute a query operation against the given index.
        """
        self.no_single_model()
        return self.with_query(self.get_query().query_with_index(index_name, consistent_read=consistent_read, check_query=check_query))

    def get_query(self) -> Query:
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
        raise NotImplementedError("The Dynamodb model does not support sorting via the 'sort_by' method because dynamodb does not support generic sorting.  Use the 'query_with_index' method instead.")
