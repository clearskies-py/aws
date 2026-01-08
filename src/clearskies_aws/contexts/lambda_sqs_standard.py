from __future__ import annotations

import traceback
from typing import Any

from clearskies.authentication import Public
from clearskies.contexts.context import Context

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
    ```

    """

    def __call__(  # type: ignore[override]
        self, event: dict[str, Any], context: dict[str, Any], url: str = "", request_method: str = ""
    ) -> dict[str, Any]:
        item_failures = []
        for record in event["Records"]:
            try:
                self.execute_application(
                    LambdaSqsStandardInputOutput(record, event, context, url=url, request_method=request_method)
                )
            except Exception as e:
                item_failures.append({"itemIdentifier": record["messageId"]})

        if item_failures:
            return {
                "batchItemFailures": item_failures,
            }
        return {}
