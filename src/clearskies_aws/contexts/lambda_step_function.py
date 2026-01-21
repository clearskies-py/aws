from __future__ import annotations

from typing import Any, Callable

from clearskies.contexts.context import Context

from clearskies_aws.input_outputs.lambda_step_function import LambdaStepFunction as LambdaStepFunctionInputOutput


class LambdaStepFunction(Context):
    """
    Execute a Lambda invoked from AWS Step Functions.

    This context is used when your clearskies application is running in a Lambda that is
    invoked from an AWS Step Functions state machine. It supports extracting variables
    assigned in the Step Functions state and exposing them through the clearskies
    Environment class.

    ### Usage

    Basic usage:

    ```python
    import clearskies
    import clearskies_aws


    def my_function(request_data, environment):
        # Access extracted environment variables
        business_name = environment.get("BUSINESS_NAME")
        return {"business": business_name, "data": request_data}


    lambda_step_function = clearskies_aws.contexts.LambdaStepFunction(
        clearskies.endpoints.Callable(
            my_function,
            return_standard_response=False,
        ),
        environment_keys=["BUSINESS_NAME", "GITLAB_AUTH_KEY"],
    )


    def lambda_handler(event, context):
        return lambda_step_function(event, context)
    ```

    ### Configuration Options

    The `environment_keys` parameter accepts three types:

    **1. List of keys** - Extract specific keys from the event:
    ```python
    clearskies_aws.contexts.LambdaStepFunction(
        endpoint, environment_keys=["BUSINESS_NAME", "GITLAB_AUTH_KEY"]
    )
    ```

    **2. Mapping dict** - Map event keys to environment variable names:
    ```python
    clearskies_aws.contexts.LambdaStepFunction(
        endpoint,
        environment_keys={
            "BUSINESS_NAME": "BUSINESS_NAME",  # same name
            "GITLAB_AUTH_KEY": "GITLAB_TOKEN_PATH",  # rename
        },
    )
    ```

    **3. Callable** - Full control via a function:
    ```python
    def extract_env_vars(event):
        return {
            "BUSINESS_NAME": event.get("BUSINESS_NAME"),
            "GITLAB_KEY": event.get("GITLAB_AUTH_KEY"),
        }


    clearskies_aws.contexts.LambdaStepFunction(endpoint, environment_keys=extract_env_vars)
    ```

    ### Context Specifics

    When using this context, the following named parameters become available to inject
    into any callable invoked by clearskies:

    |         Name           |        Type        |           Description                |
    |:----------------------:|:------------------:|:------------------------------------:|
    |        `event`         |  `dict[str, Any]`  | The lambda `event` object            |
    |       `context`        |  `dict[str, Any]`  | The lambda `context` object          |
    |   `invocation_type`    |       `str`        | Always `"step-functions"`            |
    |    `function_name`     |       `str`        | The name of the lambda function      |
    |   `function_version`   |       `str`        | The function version                 |
    |      `request_id`      |       `str`        | The AWS request id for the call      |
    |    `states_context`    |  `dict[str, Any]`  | The Step Functions $states context   |
    | `extracted_environment`|  `dict[str, str]`  | The extracted environment variables  |

    """

    def __init__(
        self,
        endpoint,
        environment_keys: list[str] | dict[str, str] | Callable[[dict[str, Any]], dict[str, str]] | None = None,
        **kwargs,
    ):
        super().__init__(endpoint, **kwargs)
        self._environment_keys = environment_keys

    def __call__(  # type: ignore[override]
        self,
        event: dict[str, Any],
        context: dict[str, Any],
        request_method: str = "",
        url: str = "",
    ) -> dict[str, Any]:
        input_output = LambdaStepFunctionInputOutput(
            event,
            context,
            request_method=request_method,
            url=url,
            environment_keys=self._environment_keys,
        )

        # Merge extracted environment into the DI container
        if input_output.extracted_environment:
            self._merge_environment(input_output.extracted_environment)

        return self.execute_application(input_output)

    def _merge_environment(self, extracted_env: dict[str, str]) -> None:
        """Merge extracted environment variables into the DI container's Environment."""
        # Get the environment from the DI container with caching enabled
        # so that subsequent builds return the same instance with our values
        environment = self.di.build("environment", cache=True)
        for key, value in extracted_env.items():
            environment.set(key, value)
