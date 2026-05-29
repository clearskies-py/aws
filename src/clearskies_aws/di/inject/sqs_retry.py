"""Injectable for SQS retry helper."""

from __future__ import annotations

from typing import TYPE_CHECKING

from clearskies_aws.di.inject.client import Client

if TYPE_CHECKING:
    from clearskies_aws.clients import BaseAwsClient
    from clearskies_aws.helpers import SqsRetry as SqsRetryHelper


class SqsRetry(Client):
    """
    Injectable for SQS retry helper.

    Automatically provides queue context (URL, receipt handle, receive count)
    when used in Lambda SQS context. The helper provides methods for scheduling
    retries with exponential backoff using visibility timeout extension.

    Example:
        Basic usage in a handler

        ```python
        from clearskies_aws.di import inject


        class MyHandler:
            sqs_retry = inject.SqsRetry()

            def __call__(self, request_data):
                if not ready():
                    self.sqs_retry.retry_later("Not ready")
                    return
                # process...
        ```

    Example:
        Checking retry eligibility

        ```python
        from clearskies_aws.di import inject


        class MyHandler:
            sqs_retry = inject.SqsRetry()

            def __call__(self, request_data):
                if not ready():
                    if self.sqs_retry.should_retry():
                        self.sqs_retry.retry_later()
                    else:
                        log_error("Max retries exceeded")
                    return
        ```
    """

    @property
    def client_class(self) -> type[BaseAwsClient]:
        from clearskies_aws.clients import SqsClient

        return SqsClient

    def __get__(self, instance, parent) -> SqsRetryHelper:
        if instance is None:
            return self  # type: ignore

        return self.build_client(instance)  # type: ignore
