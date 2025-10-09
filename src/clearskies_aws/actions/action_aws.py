from __future__ import annotations

import json
import logging
from abc import ABC
from collections import OrderedDict
from types import ModuleType
from typing import Callable, Generic, TypeVar

from botocore.client import BaseClient
from botocore.exceptions import ClientError
from clearskies.configs import Actions, Boolean, String
from clearskies.configs import Callable as ConfigCallable
from clearskies.decorators import parameters_to_properties
from clearskies.di.inject import Di, Environment
from clearskies.functional import string
from clearskies.model import Model

from clearskies_aws.di import inject

from .assume_role import AssumeRole

ClientType = TypeVar("ClientType", bound=BaseClient)


class ActionAws(Generic[ClientType], Actions):
    logging = logging.getLogger(__name__)
    boto3 = inject.Boto3()
    environment = Environment()
    di = Di()

    client: ClientType

    service_name = String(required=True)

    message_callable = ConfigCallable(required=False)

    when = ConfigCallable(required=False)

    assume_role: AssumeRole | None = None

    region = String(required=False)

    can_cache = Boolean(default=True)

    @parameters_to_properties
    def __init__(
        self,
        service_name: str,
        message_callable: Callable | None = None,
        when: Callable | None = None,
        assume_role: AssumeRole | None = None,
        region: str | None = None,
    ) -> None:
        """Set up the AWS action."""

    def __call__(self, model: Model) -> None:
        """Send a notification as configured."""
        if self.when and not self.di.call_function(self.when, model=model):
            return

        try:
            client = self._get_client()
            self._execute_action(client, model)
        except ClientError as e:
            self.logging.exception(f"Failed to retrieve client for {self.__class__.__name__} action.")
            raise e

    def _get_client(self) -> ClientType:
        """Retrieve the boto3 client."""
        if self.client and self.can_cache:
            return self.client

        if self.assume_role:
            boto3 = self.assume_role(self.boto3)
        else:
            boto3 = self.boto3

        if not self.region:
            self.region = self.default_region()
        if self.region:
            client = boto3.client(self.service_name, region_name=self.region)
        else:
            client = boto3.client(self.service_name)

        if self.can_cache:
            self.client = client
        return client

    def default_region(self):
        region = self.environment.get("AWS_REGION", silent=True)
        if region:
            return region
        region = self.environment.get("DEFAULT_AWS_REGION", silent=True)
        if region:
            return region
        return None

    def _execute_action(self, client: ClientType, model: Model) -> None:
        """Run the action."""
        pass

    def get_message_body(self, model: Model) -> str:
        """Retrieve the message for the action."""
        if self.message_callable:
            result = self.di.call_function(self.message_callable, model=model)
            if isinstance(result, dict) or isinstance(result, list):
                return json.dumps(result, default=string.datetime_to_iso)
            if not isinstance(result, str):
                raise TypeError(
                    f"The return value from the message callable for the {__name__} action must be a string, dictionary, or list. I received a "
                    + f"{type(result)} after calling '{self.message_callable.__name__}'"
                )
            return result

        model_data = OrderedDict()
        for column_name, column in model.get_columns().items():
            if not column.is_readable:
                continue
            model_data.update(column.to_json(model))
        return json.dumps(model_data, default=string.datetime_to_iso)
