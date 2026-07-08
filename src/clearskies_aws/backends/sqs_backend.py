from __future__ import annotations

import json
from typing import Any

from clearskies import Model, configs
from clearskies.query import Query
from clearskies.query.result import CountQueryResult, RecordQueryResult, RecordsQueryResult, SuccessQueryResult

from clearskies_aws.backends import backend
from clearskies_aws.di import inject


class SqsBackend(backend.Backend):
    """
    SQS backend for clearskies.

    There's not too much to this.  Just set it on your model and set the table name equal to the SQS url.

    This doesn't support setting message attributes.  The SQS call is simple enough that if you need
    those you may as well just invoke the boto3 SDK yourself.

    Note that this is a **create-only** backend.  Reading from an SQS queue is different enough from
    the way that clearskies models works that it doesn't make sense to try to make those happen here.
    If you want to do that, See the SQS context.
    """

    """Use service injectable for clean, type-safe client access."""
    sqs = inject.SqsClient()

    """
    Allow the user to configure whether the backend supports create. This backends only supports create,
    but this allows the user to disable it if they want to use the backend for some other purpose.
    """
    can_create = configs.Boolean(default=True)

    """
    Allow the user to configure whether the backend supports update.

    This backends only supports create, but this allows the user to disable it if they want to use the backend for some other purpose.
    """
    can_update = configs.Boolean(default=False)

    """
    Allow the user to configure whether the backend supports delete.

    This backends only supports create, but this allows the user to disable it if they want to use the backend for some other purpose.
    """
    can_delete = configs.Boolean(default=False)

    """
    Allow the user to configure whether the backend supports query.

    This backends only supports create, but this allows the user to disable it if they want to use the backend for some other purpose.
    """
    can_query = configs.Boolean(default=False)

    def __init__(
        self,
        aws_region: str = "",
        assume_role: backend.AssumeRoleAction | list[backend.AssumeRoleAction] = [],
        client_injection_name: str = "",
        can_create: bool | None = True,
        can_update: bool | None = False,
        can_delete: bool | None = False,
        can_query: bool | None = False,
    ):
        """Initialize the backend."""
        super().__init__(
            aws_region=aws_region,
            assume_role=assume_role,
            client_injection_name=client_injection_name,
            can_create=can_create,
            can_update=can_update,
            can_delete=can_delete,
            can_query=can_query,
        )

    def create(self, data: dict[str, Any], model: Model) -> RecordQueryResult:
        """Create a record in the SQS queue."""
        self.sqs.send_message(
            QueueUrl=model.destination_name(),
            MessageBody=json.dumps(data),
        )
        return RecordQueryResult(record={**data})

    def update(self, id: int | str, data: dict[str, Any], model: Model) -> RecordQueryResult:
        """
        Update a record in the SQS queue.

        By default this isn't supported, since SQS is a create-only backend.  If you want to support this, you can override this method in your own backend.
        """
        raise ValueError("The SQS backend only supports the create operation")

    def delete(self, id: int | str, model: Model) -> SuccessQueryResult:
        """
        Delete a record from the SQS queue.

        By default this isn't supported, since SQS is a create-only backend.  If you want to support this, you can override this method in your own backend.
        """
        raise ValueError("The SQS backend only supports the create operation")

    def count(self, query: Query) -> CountQueryResult:
        """
        Count records in the SQS queue.

        By default this isn't supported, since SQS is a create-only backend.  If you want to support this, you can override this method in your own backend.
        """
        raise ValueError("The SQS backend only supports the create operation")

    def records(self, query: Query) -> RecordsQueryResult:
        """
        Retrieve records from the SQS queue.

        By default this isn't supported, since SQS is a create-only backend.  If you want to support this, you can override this method in your own backend.
        """
        raise ValueError("The SQS backend only supports the create operation")
