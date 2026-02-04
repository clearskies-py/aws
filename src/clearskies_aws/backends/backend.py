from __future__ import annotations

from typing import Any, Callable

import clearskies
import clearskies.model
import clearskies.query
from clearskies.autodoc.schema import Schema as AutoDocSchema
from clearskies.di import inject
from clearskies.query.result import (
    CountQueryResult,
    RecordQueryResult,
    RecordsQueryResult,
    SuccessQueryResult,
)

from clearskies_aws.di.inject import boto3


class Backend(clearskies.backends.Backend, clearskies.di.InjectableProperties):
    """
    Connect models to their data since 2020.

    The backend system acts as a flexible layer between models and their data sources.  By changing the backend attached to a model,
    you change where the model fetches and saves data.  This might be a database, an in-memory data store, a dynamodb table,
    an API, and more.  This allows you to interact with a variety of data sources with the models acting as a standardized API.
    Since endpoints also rely on the models for their functionality, this means that you can easily build API endpoints and
    more for a variety of data sources with a minimal amount of code.

    Of course, not all data sources support all functionality present in the model.  Therefore, you do still need to have
    a fair understanding of how your data sources work.
    """

    supports_n_plus_one = False
    can_count = True

    boto3 = boto3.Boto3()
    environment = inject.Environment()

    def update(self, id: int | str, data: dict[str, Any], model: clearskies.model.Model) -> RecordQueryResult:
        """Update the record with the given id with the information from the data dictionary."""
        return RecordQueryResult(record={})

    def create(self, data: dict[str, Any], model: clearskies.model.Model) -> RecordQueryResult:
        """Create a record with the information from the data dictionary."""
        return RecordQueryResult(record={})

    def delete(self, id: int | str, model: clearskies.model.Model) -> SuccessQueryResult:
        """Delete the record with the given id."""
        return SuccessQueryResult()

    def count(self, query: clearskies.query.Query) -> CountQueryResult:
        """Return the number of records which match the given query configuration."""
        return CountQueryResult(count=1)

    def records(self, query: clearskies.query.Query) -> RecordsQueryResult:
        """Return a list of records that match the given query configuration."""
        return RecordsQueryResult(records=[])

    def validate_pagination_data(self, data: dict[str, Any], case_mapping: Callable[[str], str]) -> str:
        """
        Check if the given dictionary is valid pagination data for the background.

        Return a string with an error message, or an empty string if the data is valid
        """
        return ""

    def allowed_pagination_keys(self) -> list[str]:
        """
        Return the list of allowed keys in the pagination kwargs for the backend.

        It must always return keys in snake_case so that the auto casing system can
        adjust on the front-end for consistency.
        """
        return []

    def documentation_pagination_next_page_response(self, case_mapping: Callable) -> list[Any]:
        """
        Return a list of autodoc schema objects.

        It will describe the contents of the `next_page` dictionary
        in the pagination section of the response
        """
        return []

    def documentation_pagination_parameters(self, case_mapping: Callable) -> list[tuple[AutoDocSchema, str]]:
        """
        Return a list of autodoc schema objects describing the allowed input keys to set pagination.

        It should return a list of tuples, with each tuple corresponding to an input key.
        The first element in the tuple should be the schema, and the second should be the description.
        """
        return []

    def documentation_pagination_next_page_example(self, case_mapping: Callable) -> dict[str, Any]:
        """
        Return an example for next page documentation.

        Returns an example (as a simple dictionary) of what the next_page data in the pagination response
        should look like
        """
        return {}
