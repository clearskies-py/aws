from __future__ import annotations

from typing import Any, Self

import clearskies


class DynamodbQuery(clearskies.query.Query):
    """Adds in additional query operations needed by Dynamodb."""

    """
    If set, perform a query operation (instead of scan) against the index named in this property.
    """
    with_index: str = ""

    """
    Perform a consistent read against DynamoDB.

    See the docs:

    https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.ReadConsistency.html
    """
    consistent_read: bool | None = None

    """
    If True, clearskies will describe the index being queried to verify that the conditions provided can actually query it.
    """
    check_query: bool = True

    """
    The direction to sort when querying an index (ASC or DESC)
    """
    query_direction: str = ""

    """
    How to handle nulls when querying.

    This defaults to `None`, which means that the null behavior is not specificed in the query and will fall back
    on the default.  Setting to `True` will cause missing and null vaules to come first in the result set.  False
    does the opposite.
    """
    nulls_first: bool | None = None

    def __init__(
        self,
        model_class: type[clearskies.Model],
        conditions: list[clearskies.query.Condition] = [],
        joins: list[clearskies.query.Join] = [],
        sorts: list[clearskies.query.Sort] = [],
        limit: int = 0,
        group_by: str = "",
        pagination: dict[str, Any] = {},
        selects: list[str] = [],
        select_all: bool = True,
        with_index: str = "",
        consistent_read: bool | None = None,
        check_query: bool = True,
        nulls_first: bool | None = None,
    ):
        super().__init__(
            model_class,
            conditions=conditions,
            joins=joins,
            sorts=sorts,
            limit=limit,
            group_by=group_by,
            pagination=pagination,
            selects=selects,
            select_all=select_all,
        )

        self.with_index = with_index
        self.consistent_read = consistent_read
        self.check_query = check_query
        self.nulls_first = nulls_first

    def query_with_index(
        self,
        index_name: str,
        direction: str = "ASC",
        nulls_first: bool | None = None,
        consistent_read: bool | None = None,
        check_query: bool = True,
    ) -> Self:
        if direction and direction.lower() not in ["asc", "desc"]:
            raise ValueError(
                f"Invalid call to DynamodbQuery.query_with_index: 'direction' must be one of 'ASC' or 'DESC', but I received '{direction}'"
            )

        new_kwargs = self.as_kwargs()
        new_kwargs["with_index"] = index_name
        new_kwargs["consistent_read"] = consistent_read
        new_kwargs["check_query"] = check_query
        new_kwargs["nulls_first"] = nulls_first
        return self.__class__(**new_kwargs)

    def as_kwargs(self) -> dict[str, Any]:
        """Return the properties of this query as a dictionary so it can be used as kwargs when creating another one."""
        return {
            **super().as_kwargs(),
            **{
                "with_index": self.with_index,
                "consistent_read": self.consistent_read,
                "check_query": self.check_query,
                "nulls_first": self.nulls_first,
            },
        }
