from __future__ import annotations

import json
from typing import Any as TypingAny
from typing import Callable

from clearskies.configs import Any
from clearskies.di import Di
from clearskies.environment import Environment
from clearskies.exceptions import ClientError
from clearskies.input_outputs import Headers

from clearskies_aws.input_outputs import lambda_input_output


class LambdaStepFunction(lambda_input_output.LambdaInputOutput):
    """
    Input/output handler for Lambda invocations from AWS Step Functions.

    This input/output handles Lambda invocations that come from AWS Step Functions state machines.
    It provides support for extracting variables assigned in the Step Functions state and exposing
    them through the clearskies Environment class.

    The primary use case is when you want to use Step Functions variables (set via the `Assign` feature
    in JSONata expressions) as environment variables in your clearskies application. This allows you to
    reuse the same Lambda function with different configuration values passed through the state machine.

    The `environment_keys` parameter controls how variables are extracted from the event and injected
    into the environment. It accepts three types:

    **List of keys** - Extract specific keys directly from the event:

    ```python
    LambdaStepFunction(event, context, environment_keys=["BUSINESS_NAME", "API_KEY"])
    ```

    **Mapping dict** - Map event keys to different environment variable names:

    ```python
    LambdaStepFunction(
        event,
        context,
        environment_keys={
            "BUSINESS_NAME": "COMPANY_NAME",  # event["BUSINESS_NAME"] -> env["COMPANY_NAME"]
            "GITLAB_AUTH_KEY": "GITLAB_TOKEN_PATH",
        },
    )
    ```

    **Callable** - Full control via a function that can use dependency injection:

    ```python
    def extract_env_vars(event, secrets):
        # Can inject clearskies dependencies like secrets
        return {
            "BUSINESS_NAME": event.get("BUSINESS_NAME"),
            "GITLAB_KEY": secrets.get(event.get("GITLAB_AUTH_KEY")),
        }


    LambdaStepFunction(event, context, environment_keys=extract_env_vars)
    ```

    Note that when using a list or dict, all specified keys must exist in the event or a `KeyError`
    will be raised. When using a callable, it must return a dictionary or a `TypeError` will be raised.
    """

    """
    Configuration for extracting environment variables from the Step Functions event.

    Can be a list of keys to extract directly, a dict mapping event keys to environment names,
    or a callable that receives the event and returns a dict of environment variables.
    """
    environment_keys = Any(default=None)

    def __init__(
        self,
        event: dict[str, TypingAny],
        context: dict[str, TypingAny],
        request_method: str = "",
        url: str = "",
        environment_keys: list[str] | dict[str, str] | Callable[..., dict[str, TypingAny]] | None = None,
    ):
        super().__init__(event, context)

        self.environment_keys = environment_keys

        if url:
            self.url = url
            self.path = url
        else:
            self.supports_url = True
        if request_method:
            self.request_method = request_method.upper()
        else:
            self.supports_request_method = False

        self.request_headers = Headers({})

    def inject_extra_environment_variables(self, environment: Environment, di: Di) -> None:
        """
        Extract and inject environment variables from the event into the Environment.

        This method is called by the context to extract variables from the Step Functions event
        and inject them into the clearskies Environment. The extraction behavior depends on the
        type of `environment_keys`:

        - If `environment_keys` is `None`, no extraction is performed.
        - If `environment_keys` is a list, each key is extracted from the event and injected
          with the same name.
        - If `environment_keys` is a dict, each event key is mapped to the corresponding
          environment name.
        - If `environment_keys` is a callable, it is invoked via the DI container (allowing
          dependency injection) and must return a dict of environment variables.

        Raises `KeyError` if a requested key is not found in the event (for list/dict modes).
        Raises `TypeError` if a callable does not return a dictionary.
        """
        if self.environment_keys is None:
            return

        extracted: dict[str, TypingAny] = {}

        if callable(self.environment_keys):
            # Let clearskies call the callable so it can inject dependencies
            result = di.call_function(self.environment_keys, event=self.event)
            if not isinstance(result, dict):
                callable_name = getattr(self.environment_keys, "__name__", str(self.environment_keys))
                raise TypeError(
                    f"The environment_keys callable '{callable_name}' must return a dictionary, "
                    f"but returned {type(result).__name__}"
                )
            extracted = result
        elif isinstance(self.environment_keys, dict):
            for event_key, env_name in self.environment_keys.items():
                if event_key not in self.event:
                    raise KeyError(
                        f"environment_keys requested a key called `{event_key}` but this was not found in the event"
                    )
                extracted[env_name] = self.event[event_key]
        elif isinstance(self.environment_keys, list):
            for key in self.environment_keys:
                if key not in self.event:
                    raise KeyError(
                        f"environment_keys requested a key called `{key}` but this was not found in the event"
                    )
                extracted[key] = self.event[key]

        # Inject extracted values into the environment
        for key, value in extracted.items():
            environment.set(key, value)

    def has_body(self) -> bool:
        """Step Functions invocations always have a body (the event itself)."""
        return True

    def get_body(self) -> str:
        """Return the entire event as a JSON-encoded string."""
        if isinstance(self.event, (dict, list)):
            return json.dumps(self.event)
        return str(self.event)

    def respond(self, body: TypingAny, status_code: int = 200) -> TypingAny:
        """Return the response directly for Step Functions invocations (no HTTP wrapping)."""
        if isinstance(body, bytes):
            return body.decode("utf-8")
        return body

    def get_protocol(self) -> str:
        """Return the protocol identifier for Step Functions invocations."""
        return "step-functions"

    def context_specifics(self) -> dict[str, TypingAny]:
        """
        Provide Step Functions specific context data for dependency injection.

        Returns a dict containing:
        - `event`: The raw Lambda event
        - `context`: The raw Lambda context
        - `invocation_type`: Always "step-functions"
        - `function_name`: The Lambda function name
        - `function_version`: The Lambda function version
        - `request_id`: The AWS request ID
        - `states_context`: The Step Functions $states context (if present)
        """
        states_context = self.event.get("$states", {})

        return {
            **super().context_specifics(),
            "invocation_type": "step-functions",
            "function_name": self.context.get("function_name"),
            "function_version": self.context.get("function_version"),
            "request_id": self.context.get("aws_request_id"),
            "states_context": states_context,
        }

    @property
    def request_data(self) -> dict[str, TypingAny] | list[TypingAny] | None:
        """Return the event directly as request data."""
        return self.event

    def json_body(
        self, required: bool = True, allow_non_json_bodies: bool = False
    ) -> dict[str, TypingAny] | list[TypingAny] | None:
        """Return the event as JSON data (already parsed from the Lambda invocation)."""
        if required and not self.event:
            raise ClientError("Request body was not valid JSON")
        return self.event
