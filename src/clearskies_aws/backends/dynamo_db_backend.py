# type: ignore
from __future__ import annotations

import base64
import json
from decimal import Decimal
from typing import Any, Callable

from clearskies.autodoc.schema import String as AutoDocString
from clearskies.columns import Boolean, Float
from clearskies.di import inject
from types_boto3_dynamodb import DynamoDBClient
from clearskies import Column

from clearskies.backends import CursorBackend
from clearskies_aws.backends import backend
from clearskies_aws.di import inject
from clearskies_aws.cursors import Dynamodb as DynamodbCursor

class DynamoDbBackend(backend.Backend, CursorBackend):
    """
    Manage records in an AWS Dynamo DB table!

    ## Usage

    By default the DynamoDb backend will point to DynamoDB in the region specified by the `AWS_REGION` or
   `AWS_DEFAULT_REGION`environment variable (in that order).  If you need to use a different region than
    defined via the environment, then you can explicitly provide the region to the backend.  Per cleraskies
    norms, the table name will be automatically determined by converting the class name to snake case
    and making it plural, or you can specify this directly by overriding the `destination_name` class
    method on the model class.  So, in this example:

    ```
    import clearskies
    import clearskies_aws

    class MyModel(clearskies.Model):
        id_column_name = "id"
        backend = clearskies_aws.backends.DynamoDbBackend()

        id = clearskies.columns.Uuid()
    ```

    the model will work with a dynamodb table called `my_models`.

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
    name of the index to query on.  Your AWS principal must also have permission to `dynamodb:DescribeTable`
    for the table associated with your model.  Clearskies will use fetch the details of your desired index so it
    can verify you have provided all the necessary information to execute the query, as well as to understand
    how to properly make use of the index.

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

    | Name      |
    |-----------|
    | Snake     |
    | Cat       |
    | Alligator |
    | Dog       |

    and your record size is such that only two records can be processed at a time.  If you performed a scan
    operation and asked DynamoDB to sort by name asc, you would get two result sets with the following records
    returned:

     1. `["Cat", "Snake"]`
     2. `["Alligator", "Dog"]`

    Remember that the DynamoDB backend will always execute a scan operation unless you use the `query_with_index`
    method to explicitly state wihch index you want to query on.
    """

    dynamodb = inject.DynamoDbClient()

    @property
    def cursor(self):
        """
        Lazily inject and return the dynamodb cursor instance.

        Returns
        -------
            The cursor object used for executing dynamodb queries.
        """
        if not hasattr(self, "_cursor"):
            self._cursor = self.di.build(DynamodbCursor, self.dynamodb)
        return self._cursor

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
