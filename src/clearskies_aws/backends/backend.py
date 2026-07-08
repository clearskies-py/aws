from __future__ import annotations

from typing import Any, Callable

import clearskies
import clearskies.model
import clearskies.query
from clearskies.autodoc.schema import Schema as AutoDocSchema
from clearskies.configs import String
from clearskies.decorators import parameters_to_properties
from clearskies.di.inject import Environment
from clearskies.query.result import (
    CountQueryResult,
    RecordQueryResult,
    RecordsQueryResult,
    SuccessQueryResult,
)

from clearskies_aws.actions import AssumeRole as AssumeRoleAction
from clearskies_aws.configs import AssumeRole, Region
from clearskies_aws.di import inject


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

    boto3 = inject.Boto3()
    environment = Environment()

    """
    The region of AWS to work in.

    Set the region for your backend resource.  If unset, the backend will fallback on the `AWS_REGION` or
    `AWS_DEFAULT_REGION`environment variable.
    """
    aws_region = Region()

    """
    Assume role configuration: either a single assume role declaration or a list of them to assume a chain of roles.

    The assume role operation is described via an instance of `clearskies_aws.actions.AssumeRole()`.

    Example usage of a single assume role:

    ```
    import clearskies
    import clearskies_aws
    from clearskies_aws.actions import AssumeRole

    class MyModel(clearskies.Model):
        id_column_name = "id"
        backend = clearskies_aws.backends.DynamoDbBackend(
            assume_role=AssumeRole(
                role_arn="arn:aws:123456789012:iam:role/name",
                external_id="12345",
            )
        )

        id = clearskies.columns.Uuid()
        name = clearskies.columns.String()
    ```

    Or a chain of assume roles:

    ```
    import clearskies
    import clearskies_aws
    from clearskies_aws.actions import AssumeRole

    class MyModel(clearskies.Model):
        id_column_name = "id"
        backend = clearskies_aws.backends.DynamoDbBackend(
            assume_role=[
                AssumeRole(
                    role_arn="arn:aws:123456789012:iam:role/name",
                    external_id="12345",
                ),
                AssumeRole(
                    role_arn="arn:aws:210987654321:iam:role/name",
                    external_id="54321",
                ),
            ]
        )

        id = clearskies.columns.Uuid()
        name = clearskies.columns.String()
    ```

    """
    assume_role = AssumeRole()

    """
    A dependency injection name from which the client can be fetched.

    Instead of specifying region and assume role settins on the backend, you can specify a
    dependency injection name that the client can be fetched from.  This can be handy for increased
    flexibilty, since the client settings can be configured at the context level/overriden during testing/etc...

    ```
    import clearskies
    import clearskies_aws
    from clearskies_aws.actions import AssumeRole

    class MyModel(clearskies.Model):
        id_column_name = "id"
        backend = clearskies_aws.backends.DynamoDbBackend(
            client_injection_name="dynamodb_client"
        )

        id = clearskies.columns.Uuid()
        name = clearskies.columns.String()

    wsgi = clearskies.contexts.WsgiRef(
        clearskies.endpoints.Callable(
            lambda my_models: my_models.create({"name":"hello world!"})
        ),
        bindings={
            "dynamodb_client": clearskies_aws.clients.DynamodbClient
        }
    )
    wsgi()
    ```
    """
    client_injection_name = String()

    @parameters_to_properties
    def __init__(
        self,
        aws_region: str = "",
        assume_role: AssumeRoleAction | list[AssumeRoleAction] = [],
        client_injection_name: str = "",
        can_create: bool | None = True,
        can_update: bool | None = True,
        can_delete: bool | None = True,
        can_query: bool | None = True,
    ):
        """Initialize the backend."""
        self.finalize_and_validate_configuration()

    def update(self, id: int | str, data: dict[str, Any], model: clearskies.model.Model) -> RecordQueryResult:
        """Update the record with the given id with the information from the data dictionary."""
        raise NotImplementedError(f"The backend {self.__class__.__name__} doesn't support updates")

    def create(self, data: dict[str, Any], model: clearskies.model.Model) -> RecordQueryResult:
        """Create a record with the information from the data dictionary."""
        raise NotImplementedError(f"The backend {self.__class__.__name__} doesn't support creation")

    def delete(self, id: int | str, model: clearskies.model.Model) -> SuccessQueryResult:
        """Delete the record with the given id."""
        raise NotImplementedError(f"The backend {self.__class__.__name__} doesn't support deletion")

    def count(self, query: clearskies.query.Query) -> CountQueryResult:
        """Return the number of records which match the given query configuration."""
        raise NotImplementedError(f"The backend {self.__class__.__name__} doesn't support counting.")

    def records(self, query: clearskies.query.Query) -> RecordsQueryResult:
        """Return a list of records that match the given query configuration."""
        raise NotImplementedError(f"The backend {self.__class__.__name__} doesn't support fetching records")

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
