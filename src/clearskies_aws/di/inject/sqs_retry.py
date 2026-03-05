"""Injectable for SQS retry helper."""

from __future__ import annotations

from typing import TYPE_CHECKING

from clearskies.di.injectable import Injectable

if TYPE_CHECKING:
    from clearskies_aws.helpers import SqsRetry as SqsRetryHelper


class SqsRetry(Injectable):
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

    def __get__(self, instance, parent) -> SqsRetryHelper:
        if instance is None:
            return self  # type: ignore

        # Build the helper with context
        # The context will have already bound queue_url, receipt_handle, receive_count
        return self._di.build("sqs_retry", cache=False)
