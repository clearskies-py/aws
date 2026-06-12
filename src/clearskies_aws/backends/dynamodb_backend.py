# type: ignore
from __future__ import annotations

import base64
import json
from decimal import Decimal
from typing import Any, Callable

from clearskies import Column, Model, configs
from clearskies.autodoc.schema import String as AutoDocString
from clearskies.backends import CursorBackend
from clearskies.columns import Boolean, Float, Integer
from clearskies.query import Condition, ParsedCondition, Query
from clearskies.query.result import (
    CountQueryResult,
    RecordQueryResult,
    RecordsQueryResult,
    SuccessQueryResult,
)
from types_boto3_dynamodb import DynamoDBClient

from clearskies_aws.actions.assume_role import AssumeRole as AssumeRoleAction
from clearskies_aws.backends import backend
from clearskies_aws.di import inject
from clearskies_aws.cursors import Dynamodb as DynamodbCursor


class DynamodbBackend(CursorBackend, backend.Backend):
    """
    Manage records in an AWS Dynamo DB table.

    ## Usage

    By default the DynamoDb backend will point to DynamoDB in the region specified by the `AWS_REGION` or
    `AWS_DEFAULT_REGION`environment variable (in that order).  If your connection details are different you
    can provide aws_region/assume_role/client_injection_name to the backend.

    Per cleraskies norms, the table name will be automatically determined by converting the class name to snake
    case and making it plural, or you can specify this directly by overriding the `destination_name` class
    method on the model class.  So, in this example:

    ```
    import clearskies
    import clearskies_aws


    class MyModel(clearskies_aws.models.DynamodbModel):
        id_column_name = "id"
        backend = clearskies_aws.backends.DynamodbBackend()

        id = clearskies.columns.Uuid()
    ```

    the model will work with a dynamodb table called `my_models`.  Note that there is an associated model you
    should extend when using Dynamodb.  This model comes with an extra method - `query_with_index` that will
    tell clearskies to execute a query operation (instead of a scan) and specify which index to use for the
    query.

    ## Understanding Indexes and Search/Sorting

    It's important to understand that the filtering and sorting options in DynamoDB are different than
    your typical SQL database.  Rather, DynamoDB breaks things down to two main operations: `scan` and `query`
    operations.

    ### Query

    A query operation acts on a defined index and leads to high performance searching and sorting,
    but the search and sort operation is strictly limited to what the given index can support (based on the
    hash and range attributes defined on the index).  To execute a query operation, you have to explicitly
    specify which named index you want to use.  For a given index, DynamoDB will always filter on the hash
    column and **then** sort on the range column.  With DynamoDB, it's not possible to sort with an index without
    first filtering.  As a result, a simple query that (conceptually) looks like:
    `SELECT * FROM table ORDER BY column ASC` isn't possible.  You have to always have a WHERE condition in order
    to sort on an index.

    To support this, the DynamoDB backend comes with a DynamoDB model that adds an additonal method to your model:
    `query_with_index`.  When you wish to perform a query operation, you must call this method and specify the
    name of the index to query on.

    Under the hood, the dynamodb backend uses PartiQL which defaults to the primary index for the table.  So,
    if your primary index is for the id column of your table, searches using `id=value` will use a query
    operation by default.

    ### Scan

    A scan operation operates on a subset of your records without a supporting index.  DynamoDB will instead
    fetch enough records to fill up its maximum record size (defined by the total amount of data - not some
    specific number of records) and then apply any filters on that subset.  As a result, a scan operation is
    both more expensive (in terms of query time and cost) and doesn't return comprehensive results in the
    same way that an SQL database would.  E.g., if you were to ask DynamoDB to sort your table on some column,
    descending, and your have more records than can fit into a single result set, then there is no guarantee
    that the first record you are given will have the highest value for your sort column.  The first record
    in each result set will always have the highest value in that set, but the actual record with the highest
    value can show up in any result set - it's not guaranteed to be in the first.

    For example, imagine you have the following table, organized as so:

    | Age       |
    |-----------|
    | 5         |
    | 1         |
    | 18        |
    | 20        |

    and your record size is such that only two records can be processed at a time.  If you performed a scan
    operation and asked DynamoDB to find records with `Age>10`, you would get two result sets with the following records
    returned:

     1. `[]`
     2. `[18, 20]`

    To emphasize: DynamoDB can only process two records at a time (in this hypothetical example).  As a result, it
    pulls two records out of the table, finds that none of them matches the conditions, and so returns an empty
    result set.  However, this would come with a pagination token because there are more records.  You could then
    use that to fetch the next page of results, which will then bring back records that do match.

    Keep in mind that the DynamoDB backend will always execute a scan operation unless you use the `query_with_index`
    method to explicitly state wihch index you want to query on (with a minor exception for searching on the id
    column if it is the primary index for the table).
    """

    """
    The list of columns that are indexed in the dynamodb table.

    During an update operation, you must provide all indexed columns to dynamodb even if they are not
    being changed.  For example, consider a table with two indexes: one on id with a sort key of 'created_at'
    and another on 'category' with a sort key of 'age'.  If you were to update some other column, dynamodb
    forces you to execute the equivalent of:

    ```
    UPDATE my_table SET some_column='asdf' where id='1-2-3-4' AND created_at='02/02/2026' AND category='Toys' AND age=25;
    ```

    To support this, clearskies automatically provides all current column values as `column=value` search clauses
    in the `WHERE` condition of the corresponding update call, even though most probably aren't in indexes.
    If desired, you can use this property to tell clearskies which columns are actually present in indexes,
    so that only those columns will be sent along on an update operation.

    Note that if you set this configuration but don't actually provide all the indexed columns, then you will
    get a ValidationException from dynamodb about "Where clause does not contain a mandatory equality on all key attributes"
    """
    indexed_columns = configs.StringList()

    dynamodb = inject.DynamodbClient()

    def __init__(
        self,
        aws_region: str = "",
        assume_role: AssumeRoleAction | list[AssumeRoleAction] = [],
        client_injection_name: str = "",
        indexed_columns: list[str] = [],
    ):
        self.aws_region = aws_region
        self.assume_role = assume_role
        self.client_injection_name = client_injection_name
        self.indexed_columns = indexed_columns
        self.table_prefix = ""
        self.finalize_and_validate_configuration()

    @property
    def cursor(self):
        """
        Lazily inject and return the dynamodb cursor instance.

        Returns
        -------
            The cursor object used for executing dynamodb queries.
        """
        if not hasattr(self, "_cursor") or not self._cursor:
            self._cursor = DynamodbCursor(self.dynamodb)
        return self._cursor

    def records(self, query: Query) -> RecordsQueryResult:
        sql, parameters = self.as_sql(query)
        ### RIGHT HERE!!! I need to pull back the records and the pagination data from the cursor
        self.cursor.execute(sql, parameters)
        records = [row for row in self.cursor]
        next_page_data = None
        limit = query.limit
        if self.cursor.next_token:
            next_page_data = {"next_token": self.cursor.next_token}
        return RecordsQueryResult(records=records, next_page_data=next_page_data)

    def as_sql(self, query: Query) -> tuple[str, tuple[Any]]:
        """
        Convert a query to SQL.

        The rules for building PartiQL are just different enough that it's easier to modify this function,
        even though there is a fair amount of overlap.  In particular:

         1. Pagination information (limit and next_token) doesn't go in the query, but is provided directly to boto3
         2. Columns don't get table name prefixes, because all queries are always on a single table
         3. Sorting information comes from elsewhere in the query, since sorts directives correspond to index names, not columns
         4. Parameters are list of dictionaries rather than a tuple of values
        """
        table_name = query.model_class.destination_name()
        self.logger.debug(f"Generating SQL for table: {table_name} from model: {query.model_class.__name__}")

        # condition values usually come across as strings, which is perfectly fine for databases in general,
        # but dynamodb is more picky about this and needs everything to have the correct type.  Therefore,
        # go through our conditions and force convert things to the right type (mostly, I just care about integers).
        columns = query.model_class.get_columns()
        for condition in query.conditions:
            if condition.column_name not in columns:
                continue
            column = columns[condition.column_name]
            if isinstance(column, Integer):
                for index, value in enumerate(condition.values):
                    condition.values[index] = int(condition.values[index])

        wheres, parameters = self.conditions_as_wheres_and_parameters(
            query.conditions, query.model_class.destination_name()
        )
        select_parts = []
        if query.select_all:
            select_parts.append("*")
        if query.selects:
            select_parts.extend(query.selects)
        select = ", ".join(select_parts)

        order_by = ""
        if query.sorts:
            if len(query.sorts) > 1:
                raise ValueError(
                    "Dynamodb only supports sorting by a single column, but two sort directives were present in the query"
                )
            sort = query.sorts[0]
            if sort.table_name and sort.table_name != query.model_class.destination_name():
                raise ValueError(
                    f"Dynamodb does not support sorting by any table except the primary table, but this query wants to sort on '{sort.table_name}' rather than '{query.model_class.destination_name()}'"
                )
            if not hasattr(query, "with_index") or not query.with_index:
                more_error = ""
                if not hasattr(query.model_class, "query_with_index"):
                    more_error = ". Also, your model class must extend clearskies_aws.models.DynamodbModel, which currently it doesn't."
                raise ValueError(
                    f"Dynamodb only supports sorting when using an index and executing a query request.  However, no index has been specified.  You must call '{query.model_class.__name__}.query_with_index(index_name)' in order to sort{more_error}"
                )

            sort_parts = [query.sorts[0].column_name, query.sorts[0].direction]
            if query.nulls_first is not None:
                sort_parts.extend(["NULLS", ("FIRST" if query.nulls_first else "LAST")])

            order_by = " ORDER BY " + " ".join(sort_parts)

        # There are a few parameters that don't go in the query string itself, but which go in the call to
        # boto3.execute_statement.  This is tricky because with the way the cursor backend is designed, the
        # original query object doesn't go into the execution flow.  Instead, just the query string and the
        # parameters.  So, to pass this information around (without re-writing the query flow in clearskies)
        # we'll shove these query options into the parameters for the query, and then the dynamodb cursor
        # will pull them out and process them as needed.  This works out because the parameters are already
        # a list of dicts, so we can at least pass them along in an easy-to-recognize way.  I use all
        # capitals for the key names here because dynamodb does that for it's own key names in the parameters.
        # it doesn't really matter, but :shrug:.
        if query.limit:
            parameters += ({"LIMIT": query.limit},)  #  type: ignore
        if hasattr(query, "consistent_read") and isinstance(query.consistent_read, bool):
            parameters += ({"CONSISTENT_READ": query.consistent_read},)  #  type: ignore
        if query.pagination.get("next_token"):
            parameters += ({"NEXT_TOKEN": query.pagination.get("next_token")},)  #  type: ignore

        table_name = self._finalize_table_name(
            ".".join([table_name, query.with_index])
            if hasattr(query, "with_index") and query.with_index
            else table_name
        )

        return (
            f"SELECT {select} FROM {table_name}{wheres}{order_by}".strip(),
            parameters,
        )

    def conditions_as_wheres_and_parameters(
        self, conditions: list[Condition], default_table_name: str
    ) -> tuple[str, tuple[Any]]:
        parameters = ()
        if not conditions:
            return ("", parameters)

        where_parts = []
        for condition in conditions:
            for value in condition.values:
                parameters += (self.cursor.as_partiql_parameter(value),)
            column = condition.column_name
            where_parts.append(
                condition._with_placeholders(
                    column,
                    condition.operator,
                    condition.values,
                    escape=False,
                    placeholder=self.cursor.value_placeholder,
                )
            )
        return (" WHERE " + " AND ".join(where_parts), parameters)

    def update(self, id: int | str, data: dict[str, Any], model: Model) -> RecordQueryResult:
        query_parts = []
        parameters = []
        for key, val in data.items():
            query_parts.append(self.cursor.column_equals_with_placeholder(key))
            parameters.append(val)
        updates = ", ".join(query_parts)

        # update the record
        table_name = self._finalize_table_name(model.destination_name())
        column_equals = []

        # dynamodb requires us to include a WHERE COLUMN=VALUE for any column that is in an index (either the key or sort).
        # This is tricky, since we don't know what the indexes are so we don't know what columns we have to include.
        # Therefore, include everything unless the developer has told us what we need.
        columns = self.indexed_columns if self.indexed_columns else model.get_columns()
        for key, value in model.get_raw_data().items():
            if key not in columns:
                continue
            column_equals.append(self.cursor.column_equals_with_placeholder(key))
            parameters.append(value)

        self.cursor.execute(
            f"UPDATE {table_name} SET {updates} WHERE " + " AND ".join(column_equals), tuple(parameters)
        )

        # and now query again to fetch the updated record.
        records_response = self.records(
            Query(model.__class__, conditions=[ParsedCondition(model.id_column_name, "=", [str(id)])])
        )
        records = records_response.data
        return RecordQueryResult(record=records[0])

    def create(self, data: dict[str, Any], model: Model) -> RecordQueryResult:
        # for some reason the insert statement for partiql requires a single quote, not an apostrophe
        escape = "'"
        parts = []
        for key in data.keys():
            parts.append(f"{escape}{key}{escape}: {self.cursor.value_placeholder}")
        inserts = ", ".join(parts)

        table_name = self._finalize_table_name(model.destination_name())
        self.cursor.execute(
            "INSERT INTO " + table_name + " VALUE {" + inserts + "}",
            tuple([self.cursor.as_partiql_parameter(value) for value in data.values()]),
        )
        new_id = data.get(model.id_column_name)
        if not new_id:
            new_id = self.cursor.lastrowid
        if not new_id:
            raise ValueError("I can't figure out what the id is for a newly created record :(")

        records_response = self.records(
            Query(model.__class__, conditions=[ParsedCondition(model.id_column_name, "=", [new_id])])
        )
        records = records_response.data
        return RecordQueryResult(record=records[0])

    def delete(self, id: int | str, model: Model) -> SuccessQueryResult:
        table_name = self._finalize_table_name(model.destination_name())

        # dynamodb requires us to include a WHERE COLUMN=VALUE for any column that is in an index (either the key or sort).
        # This is tricky, since we don't know what the indexes are so we don't know what columns we have to include.
        # Therefore, include everything unless the developer has told us what we need.
        parameters = []
        column_equals = []
        columns = self.indexed_columns if self.indexed_columns else model.get_columns()
        for key, value in model.get_raw_data().items():
            if key not in columns:
                continue
            column_equals.append(self.cursor.column_equals_with_placeholder(key))
            parameters.append(value)

        self.cursor.execute(f"DELETE FROM {table_name} WHERE " + " AND ".join(column_equals), tuple(parameters))
        return SuccessQueryResult()

    def validate_pagination_kwargs(self, kwargs: dict[str, Any], case_mapping: Callable) -> str:
        extra_keys = set(kwargs.keys()) - set(self.allowed_pagination_keys())
        if len(extra_keys):
            key_name = case_mapping("next_token")
            return "Invalid pagination key(s): '" + "','".join(extra_keys) + f"'.  Only '{key_name}' is allowed"
        if "next_token" not in kwargs:
            key_name = case_mapping("next_token")
            return f"You must specify '{key_name}' when setting pagination"
        # the next token should be a urlsafe-base64 encoded JSON string
        try:
            json.loads(base64.urlsafe_b64decode(kwargs["next_token"]))
        except:
            key_name = case_mapping("next_token")
            return "The provided '{key_name}' appears to be invalid."
        return ""

    def allowed_pagination_keys(self) -> list[str]:
        return ["next_token"]

    def documentation_pagination_next_page_response(self, case_mapping: Callable) -> list[Any]:
        return [AutoDocString(case_mapping("next_token"))]

    def documentation_pagination_next_page_example(self, case_mapping: Callable) -> dict[str, Any]:
        return {case_mapping("next_token"): ""}

    def documentation_pagination_parameters(self, case_mapping: Callable) -> list[tuple[Any]]:
        return [(AutoDocString(case_mapping("next_token"), example=""), "A token to fetch the next page of results")]

    def column_from_backend(self, column: Column, value: Any) -> Any:
        """We have a couple columns we want to override transformations for."""
        # We're pretty much ignoring the BOOL type for dynamodb, because it doesn't work in indexes
        # (and 99% of the time when I have a boolean, it gets used in an index).  Therefore,
        # convert boolean values to "0", "1".
        if isinstance(column, Boolean):
            if value == "1":
                return True
            elif value == "0":
                return False
            else:
                return bool(value)
        return super().column_from_backend(column, value)

    def validate_pagination_data(self, data: dict[str, Any], case_mapping: Callable) -> str:
        extra_keys = set(data.keys()) - set(self.allowed_pagination_keys())
        if len(extra_keys):
            key_name = case_mapping("next_token")
            return "Invalid pagination key(s): '" + "','".join(extra_keys) + f"'.  Only '{key_name}' is allowed"
        if "next_token" not in data:
            key_name = case_mapping("next_token")
            return f"You must specify '{key_name}' when setting pagination"
        return ""

    def column_to_backend(self, column: Column, backend_data: dict[str, Any]) -> dict[str, Any]:
        """We have a couple columns we want to override transformations for."""
        # most importantly, there's no need to transform a JSON column in either direction
        if isinstance(column, Boolean):
            if column.name not in backend_data:
                return backend_data
            as_string = "1" if bool(backend_data[column.name]) else "0"
            return {**backend_data, column.name: as_string}
        if isinstance(column, Float):
            if column.name not in backend_data:
                return backend_data
            return {**backend_data, column.name: Decimal(backend_data[column.name])}
        return column.to_backend(backend_data)
