from __future__ import annotations

import json
from typing import Any

from clearskies import Model
from clearskies.query import Query
from clearskies.query.result import CountQueryResult, RecordQueryResult, RecordsQueryResult, SuccessQueryResult
from types_boto3_sqs import SQSClient

from clearskies_aws.backends import backend


class SqsBackend(backend.Backend):
    """
    SQS backend for clearskies.

    There's not too much to this.  Just set it on your model and set the table name equal to the SQS url.

    This doesn't support setting message attributes.  The SQS call is simple enough that if you need
    those you may as well just invoke the boto3 SDK yourself.

    Note that this is a *write-only* backend.  Reading from an SQS queue is different enough from
    the way that clearskies models works that it doesn't make sense to try to make those happen here.

    See the SQS context in this library for processing your queue data.
    """

    _sqs: SQSClient

    @property
    def sqs(self) -> SQSClient:
        if not hasattr(self, "_sqs"):
            if not self.environment.get("AWS_REGION", True):
                raise ValueError("To use SQS you must use set AWS_REGION in the .env file or an environment variable")

            self._sqs = self.boto3.client("sqs", region_name=self.environment.get("AWS_REGION", True))

        return self._sqs

    def create(self, data: dict[str, Any], model: Model) -> RecordQueryResult:
        self.sqs.send_message(
            QueueUrl=model.destination_name(),
            MessageBody=json.dumps(data),
        )
        return RecordQueryResult(record={**data})

    def update(self, id: int | str, data: dict[str, Any], model: Model) -> RecordQueryResult:
        raise ValueError("The SQS backend only supports the create operation")

    def delete(self, id: int | str, model: Model) -> SuccessQueryResult:
        raise ValueError("The SQS backend only supports the create operation")

    def count(self, query: Query) -> CountQueryResult:
        raise ValueError("The SQS backend only supports the create operation")

    def records(self, query: Query) -> RecordsQueryResult:
        raise ValueError("The SQS backend only supports the create operation")
