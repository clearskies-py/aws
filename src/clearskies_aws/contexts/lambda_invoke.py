from __future__ import annotations

from typing import Any

from clearskies.authentication import Public
from clearskies.contexts.context import Context

from clearskies_aws.input_outputs import LambdaInvoke as LambdaInvokeInputOutput


class LambdaInvoke(Context):
    """
    Execute a lambda directly.

    This context is used when your clearskies application is running in a lambda that is executed
    directly by some variation of `aws lambda invoke`.  For this context, the `event` object passed
    to the lambda handler becomes the request body in the clearskies application.  Note that, unlike
    other lambda execution strategies (ALB, Api Gateway, etc...) the event object is exactly equal
    to the body sent in with the lambda function.

    ### Usage

    Here's a simple example:

    ```
    import clearskies


    def my_function(request_data):
        return request_data


    lambda_invoke = clearskies_aws.contexts.LambdaInvoke(
        clearskies.endpoints.Callable(
            my_function,
            return_standard_response=False,
        )
    )


    def lambda_handler(event, context):
        return lambda_invoke(event, context)
    ```

    You can attach this to a lambda and might invoke it like so, with the following response:

    ```
    $ aws lambda invoke --function-name [function_name] --cli-binary-format raw-in-base64-out --payload '{"some":"data"}' | jq
    { "Payload": {"some": "data"} }
    ```

    Invoking a lambda doesn't happen from an http context, so there is no URL/request method/headers/etc.
    This typically means that clearskies applications in this context don't do routing and don't have
    authentication configured.  If you wanted to though, you could use `clearskies.endpoints.BodyParameterRouting`
    to setup some basic routing and let the invoking client choose a route by providing a parameter in the
    payload passed to lambda invoke.

    You can pass a URL/request method into the context when you invoke it, which is often used to simplify
    configuration: you can setup a standard clearskies applications with multiple endpoints, and then
    use that one application in multiple lambdas, specifying which endpoint to call for each lambda.
    This can also be helpful if you already have a clearskies application prepared for a standard http context
    and want to execute some subset of those endpoints in a lambda invoke context.  It looks something like this:

    ```
    import clearskies


    def some_function(request_data):
        return request_data


    def some_other_function(request_data):
        return request_data


    def something_else(request_data):
        return request_data


    lambda_invoke = clearskies_aws.contexts.LambdaInvoke(
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

    When using the lambda_invoke context, it exposes a few context specific parameters which can be injected into
    any function called by clearskies:

    |       Name         |        Type      |           Description           |
    |:------------------:|:----------------:|:-------------------------------:|
    |       `event`      | `dict[str, Any]` | The lambda `event` object       |
    |      `context`     | `dict[str, Any]` | The lambda `context` object     |
    |  `invocation_type` |      `str`       |        Always `"direct"`        |
    |   `function_name`  |      `str`       | The name of the lambda function |
    | `function_version` |      `str`       |       The function version      |
    |    `request_id`    |      `str`       | The AWS request id for the call |

    """

    def __call__(  # type: ignore[override]
        self, event: dict[str, Any], context: dict[str, Any], request_method: str = "", url: str = ""
    ) -> dict[str, Any]:
        return self.execute_application(
            LambdaInvokeInputOutput(
                event,
                context,
                request_method=request_method,
                url=url,
            )
        )
