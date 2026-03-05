from __future__ import annotations

import traceback
from typing import Any

from awslambdaric.lambda_context import LambdaContext
from clearskies.authentication import Public
from clearskies.contexts.context import Context

from clearskies_aws.di import inject as aws_inject
from clearskies_aws.exceptions import (
    MaxRetriesExceeded,
    SqsPermanentErrorException,
    SqsRetryException,
)
from clearskies_aws.input_outputs import LambdaSqsStandard as LambdaSqsStandardInputOutput


class LambdaSqsStandard(Context):
    """
    Process messages from an SQS Standard Queue with Lambda.

    Use this context when your application lives in a Lambda and is attached to an SQS standard
    queue.  Lambda always uses batch processing in this case, and will invoke your clearskies application
    with a batch of messags.  This clearskies context will then in turn invoke your application once
    for every batched message.  As a result, `request_data` will contain the contents of an individual message
    from the queue, rather than the original group of batched events from Lambda.  If any exception is thrown,
    every other message in the queue will still be sent to your application, and clearskies will inform
    AWS that the message and question failed to process.

    ### Usage

    Here's a very simple example:

    ```
    import clearskies


    def some_function(request_data):
        return print(request_data)


    lambda_sqs = clearskies_aws.contexts.LambdaSqsStandard(
        clearskies.endpoints.Callable(
            some_function,
        ),
    )


    def lambda_handler(event, context):
        return lambda_sqs(event, context)
    ```

    `lambda_handler` would then be attached to your lambda function, which is attached to some standard SQS.

    Like the other lambda contexts which don't exist in an HTTP world, you can also attach a clearskies application
    with routing and hard-code the path to invoke inside the lambda handler itself.  This is handy if you have
    a few related lambdas with similar configuration (since you only have to build a single application) or if
    you have an application that already exists and you want to invoke some specific endpoint with an SQS:

    ```
    import clearskies


    def some_function(request_data):
        return request_data


    def some_other_function(request_data):
        return request_data


    def something_else(request_data):
        return request_data


    lambda_invoke = clearskies_aws.contexts.LambdaSqsStandard(
        clearskies.endpoints.EndpointGroup(
            [
                clearskies.endpoints.Callable(
                    some_function,
                    url="some_function",
                ),
                clearskies.endpoints.Callable(
                    some_other_function,
                    url="some_other_function",
                ),
                clearskies.endpoints.Callable(
                    something_else,
                    url="something_else",
                ),
            ]
        )
    )


    def some_function_handler(event, context):
        return lambda_invoke(event, context, url="some_function")


    def some_other_function_handler(event, context):
        return lambda_invoke(event, context, url="some_other_function")


    def something_else_handler(event, context):
        return lambda_invoke(event, context, url="something_else")
    ```

    ### Context Specifics

    When using this context, the following named parameters become available to inject into any callable
    invoked by clearskies:

    ```
    |             Name            |       Type       | Description                                            |
    |:---------------------------:|:----------------:|--------------------------------------------------------|
    |           `event`           | `dict[str, Any]` | The lambda `event` object                              |
    |          `context`          | `dict[str, Any]` | The lambda `context` object                            |
    |         `message_id`        |       `str`      | The AWS message id                                     |
    |       `receipt_handle`      |       `str`      | The receipt handle                                     |
    |         `source_arn`        |       `str`      | The ARN of the SQS the lambda is receiving events from |
    |       `sent_timestamp`      |       `str`      | The timestamp when the message was sent                |
    | `approximate_receive_count` |       `str`      | The approximate receive count                          |
    |     `message_attributes`    | `dict[str, Any]` | The message attributes                                 |
    |           `record`          | `dict[str, Any]` | The full record of the message being processed         |
    |         `queue_url`         |       `str`      | The queue URL (extracted from eventSourceARN)          |
    |       `receive_count`       |       `int`      | The receive count as integer                            |
    |         `sqs_retry`         |     `SqsRetry`   | Retry helper (when configured)                         |
    ```

    ### Retry Handling

    The context now supports automatic retry handling with exponential backoff:

    Example:
        Using retry exceptions

        ```python
        from clearskies_aws.exceptions import SqsNotReadyException


        def my_handler(request_data, models):
            order = models.orders.find(request_data["order_id"])
            if not order:
                raise SqsNotReadyException("Order not found")
            # Process order...
        ```

    Example:
        Using retry helper

        ```python
        from clearskies_aws.di import inject


        class MyHandler:
            sqs_retry = inject.SqsRetry()

            def __call__(self, request_data):
                if not resource_ready():
                    self.sqs_retry.retry_later("Resource not ready")
                    return
                # Process...
        ```

    """

    def __init__(self, endpoint, max_retries: int = 5, **kwargs):
        """
        Initialize the Lambda SQS Standard context.

        Args:
            endpoint: The clearskies endpoint to execute
            max_retries: Maximum number of retries before sending to DLQ (default: 5)
            **kwargs: Additional arguments passed to parent Context
        """
        super().__init__(endpoint, **kwargs)
        self.max_retries = max_retries

    def __call__(  # type: ignore[override]
        self, event: dict[str, Any], context: LambdaContext | dict[str, Any], url: str = "", request_method: str = ""
    ) -> dict[str, Any]:
        """
        Process SQS messages with retry handling.

        Args:
            event: Lambda event containing SQS records
            context: Lambda context
            url: Optional URL for routing
            request_method: Optional HTTP method for routing

        Returns:
            Dictionary with batchItemFailures for messages that should be retried
        """
        item_failures = []
        for record in event["Records"]:
            try:
                # Extract queue context for retry helper
                queue_url = self._extract_queue_url(record.get("eventSourceARN", ""))
                receive_count = int(record.get("attributes", {}).get("ApproximateReceiveCount", 0))

                # Execute with retry context
                self.execute_application(
                    LambdaSqsStandardInputOutput(
                        record,
                        event,
                        context,
                        url=url,
                        request_method=request_method,
                        queue_url=queue_url,
                        receive_count=receive_count,
                    )
                )
            except SqsRetryException as e:
                # Handle automatic retry with visibility timeout extension
                result = self._handle_retry_exception(record, e)
                if result.get("action") == "send_to_dlq":
                    item_failures.append({"itemIdentifier": record["messageId"]})
                # If visibility was extended, don't mark as failure
            except (SqsPermanentErrorException, MaxRetriesExceeded):
                # Permanent failure or max retries - send to DLQ
                item_failures.append({"itemIdentifier": record["messageId"]})
            except Exception as e:
                # Unknown exception - standard failure handling (will retry based on SQS config)
                item_failures.append({"itemIdentifier": record["messageId"]})

        if item_failures:
            return {
                "batchItemFailures": item_failures,
            }
        return {}

    def _handle_retry_exception(self, record: dict[str, Any], exception: SqsRetryException) -> dict[str, Any]:
        """
        Handle retry exception by extending visibility timeout.

        Args:
            record: SQS message record
            exception: The retry exception raised

        Returns:
            Dictionary with action taken
        """
        receive_count = int(record.get("attributes", {}).get("ApproximateReceiveCount", 0))
        max_retries = self.max_retries

        # Check if max retries exceeded
        if receive_count >= max_retries:
            return {"action": "send_to_dlq", "reason": f"Max retries ({max_retries}) exceeded"}

        queue_url = self._extract_queue_url(record.get("eventSourceARN", ""))
        receipt_handle = record.get("receiptHandle", "")

        # Calculate delay
        delay = exception.delay if exception.delay else self._calculate_backoff(receive_count)

        # Extend visibility timeout
        try:
            # Build the SQS client wrapper and get the boto3 client
            sqs_client = self.build("sqs_client", cache=True)()
            sqs_client.change_message_visibility(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle,
                VisibilityTimeout=min(delay, 43200),  # Max 12 hours
            )
            return {"action": "visibility_extended", "delay": delay, "receive_count": receive_count}
        except Exception as e:
            # If we can't extend visibility, mark for DLQ
            return {"action": "send_to_dlq", "reason": f"Failed to extend visibility: {str(e)}"}

    @staticmethod
    def _extract_queue_url(event_source_arn: str) -> str:
        """
        Extract queue URL from SQS event source ARN.

        Args:
            event_source_arn: ARN like "arn:aws:sqs:us-east-1:123456789:myqueue"

        Returns:
            Queue URL like "https://sqs.us-east-1.amazonaws.com/123456789/myqueue"
        """
        if not event_source_arn:
            return ""

        # Parse ARN: arn:aws:sqs:region:account:queue-name
        parts = event_source_arn.split(":")
        if len(parts) != 6 or parts[2] != "sqs":
            return ""

        region = parts[3]
        account_id = parts[4]
        queue_name = parts[5]

        return f"https://sqs.{region}.amazonaws.com/{account_id}/{queue_name}"

    @staticmethod
    def _calculate_backoff(receive_count: int, base_delay: int = 10, max_delay: int = 900) -> int:
        """
        Calculate exponential backoff delay.

        Args:
            receive_count: Current receive count
            base_delay: Base delay in seconds
            max_delay: Maximum delay in seconds

        Returns:
            Calculated delay in seconds
        """
        return min(base_delay * (2 ** (receive_count - 1)), max_delay)
