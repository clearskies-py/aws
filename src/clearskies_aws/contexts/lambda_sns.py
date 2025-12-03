from __future__ import annotations

from typing import Any

from clearskies.authentication import Public
from clearskies.contexts.context import Context

from clearskies_aws.input_outputs import LambdaSns as LambdaSnsInputOutput


class LambdaSns(Context):
    """
    Execute a clearskies application when attached to a lambda triggered by SNS.

    This one is very straight-forward: just attach your clearskies application to work with an
    SNS-triggered lambda.  `request_data` provided to the clearskies application will be the
    message sent to the SNS.  Since this is no longer an http context, the various http parameters
    (url, request method, headers, and even responses) do not exist.  Routing won't exist unless
    you use `clearskies.endpoints.BodyParameterRouting` and include the route parameter in your
    SNS message body.

    ### Usage

    Here's a simple example:

    ```
    import clearskies


    def my_function(request_data):
        print(request_data)


    lambda_invoke = clearskies_aws.contexts.LambdaInvoke(
        clearskies.endpoints.Callable(
            my_function,
            return_standard_response=False,
        )
    )


    def lambda_handler(event, context):
        return lambda_invoke(event, context)
    ```

    Note the lack of a return value.  You can return a value if you want, but it will be ignored
    because SNS has no concept of a return response.

    If you have a number of Lambda/SNS handlers, you can bundle them together for ease-of-management
    and specify the URL when you invoke them:

    ```
    import clearskies


    def some_function(request_data):
        return request_data


    def some_other_function(request_data):
        return request_data


    def something_else(request_data):
        return request_data


    lambda_invoke = clearskies_aws.contexts.LambdaSns(
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

    When you use the LambdaSns context, it makes the following named parameters available
    to any callable that is invoked by clearskies:

    ```
    |    Name      |      Type        | Description                                    |
    |:------------:|:----------------:|------------------------------------------------|
    |    `event`   | `dict[str, Any]` | The lambda `event` object                      |
    |   `context`  | `dict[str, Any]` | The lambda `context` object                    |
    | `message_id` |       `str`      | The AWS message id                             |
    |  `topic_arn` |       `str`      | The ARN of the SNS topic that sent the message |
    |   `subject`  |       `str`      | Any subject attached to the SNS message        |
    |  `timestamp` |       `str`      | The timestamp when the message was sent        |
    ```
    """

    def __call__(self, event: dict[str, Any], context: dict[str, Any], request_method: str = "", url: str = ""):  # type: ignore[override]
        try:
            return self.execute_application(
                LambdaSnsInputOutput(event, context, request_method=request_method, url=url)
            )
        except Exception as e:
            print("Failed message " + event["Records"][0]["Sns"]["MessageId"] + ". Error error: " + str(e))
            raise e
