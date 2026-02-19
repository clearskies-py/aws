from __future__ import annotations

import json
import logging
from collections import OrderedDict
from typing import Callable, Generic, TypeVar

from botocore.client import BaseClient
from clearskies.action import Action
from clearskies.configs import Callable as CallableConfig
from clearskies.configurable import Configurable
from clearskies.decorators import parameters_to_properties
from clearskies.di.inject import Di, Environment
from clearskies.di.injectable_properties import InjectableProperties
from clearskies.functional import string
from clearskies.model import Model

from clearskies_aws import configs

ClientType = TypeVar("ClientType", bound=BaseClient)


class ActionAws(Generic[ClientType], Action, Configurable, InjectableProperties):
    """
    Minimal base class for AWS service actions.

    Provides only:
    - Client configuration
    - Message formatting utility (get_message_body)
    - Conditional execution (when)
    - Dependency injection setup

    Each service-specific action (SQS, SNS, SES, StepFunction) extends this class
    and implements its own __call__() method directly. All AWS client configuration
    (region, role assumption, caching) is handled by the client wrappers.
    """

    logging = logging.getLogger(__name__)
    environment = Environment()
    di = Di()

    """
    AWS client wrapper instance.

    Subclasses provide a default client (e.g., SqsClient, SnsClient).
    Can be overridden by passing a custom client instance.
    """
    client = configs.AwsClient(required=True)

    """
    Optional callable to generate the message body.

    When provided, this callable is invoked with the model as a parameter and should return
    a string, dictionary, or list. If not provided, the action will serialize the model's
    readable columns to JSON.
    """
    message_callable = CallableConfig(required=False, default=None)

    """
    Optional callable to determine if the action should execute.

    When provided, this callable is invoked with the model as a parameter and should return
    a boolean. If it returns False, the action is skipped.
    """
    when = CallableConfig(required=False, default=None)

    @parameters_to_properties
    def __init__(
        self,
        message_callable: Callable | None = None,
        when: Callable | None = None,
    ) -> None:
        """
        Set up the AWS action with configuration.

        Note: Service-specific actions will have additional parameters.
        """

    def get_message_body(self, model: Model) -> str:
        """
        Generate the message body for the action.

        If `message_callable` is configured, it's invoked with the model and its return value
        is used. Otherwise, the model's readable columns are serialized to JSON.

        Returns a JSON string representation of the message.
        """
        if self.message_callable:
            result = self.di.call_function(self.message_callable, model=model)
            if isinstance(result, dict) or isinstance(result, list):
                return json.dumps(result, default=string.datetime_to_iso)
            if not isinstance(result, str):
                callable_name = getattr(self.message_callable, "__name__", str(self.message_callable))
                raise TypeError(
                    f"The return value from the message callable must be a string, dictionary, or list. "
                    f"I received a {type(result)} after calling '{callable_name}'"
                )
            return result

        model_data = OrderedDict()
        for column_name, column in model.get_columns().items():
            if not column.is_readable:
                continue
            model_data.update(column.to_json(model))
        return json.dumps(model_data, default=string.datetime_to_iso)
