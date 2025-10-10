from __future__ import annotations

from typing import Callable

from clearskies.configs import Callable as CallableConfig
from clearskies.configs import String
from clearskies.decorators import parameters_to_properties
from clearskies.model import Model
from types_boto3_sqs import SQSClient

from . import assume_role
from .action_aws import ActionAws


class SQS(ActionAws[SQSClient]):
    queue_url = String(required=False)
    queue_url_environment_key = String(required=False)
    queue_url_callable = CallableConfig(required=False)
    message_group_id = CallableConfig(required=False)

    @parameters_to_properties
    def __init__(
        self,
        queue_url: str = "",
        queue_url_environment_key: str = "",
        queue_url_callable: Callable | None = None,
        message_callable: Callable | None = None,
        when: Callable | None = None,
        assume_role: assume_role.AssumeRole | None = None,
        message_group_id: str | Callable | None = None,
    ) -> None:
        """Set up the SQS action."""
        super().__init__(service_name="sqs", message_callable=message_callable, when=when, assume_role=assume_role)

    def configure(self):
        self.finalize_and_validate_configuration()
        queue_urls = 0
        for value in [self.queue_url, self.queue_url_environment_key, self.queue_url_callable]:
            if value:
                queue_urls += 1
        if queue_urls > 1:
            raise ValueError(
                "You can only provide one of 'queue_url', 'queue_url_environment_key', or 'queue_url_callable', but more than one were provided."
            )
        if not queue_urls:
            raise ValueError(
                "You must provide at least one of 'queue_url', 'queue_url_environment_key', or 'queue_url_callable'."
            )
        if self.message_group_id and not callable(self.message_group_id) and not isinstance(self.message_group_id, str):
            raise ValueError(
                "If provided, 'message_group_id' must be a string or callable, but the provided value was neither."
            )

    def _execute_action(self, client: SQSClient, model: Model) -> None:
        """Send a notification as configured."""
        params = {
            "QueueUrl": self.get_queue_url(model),
            "MessageBody": self.get_message_body(model),
        }
        if not params["QueueUrl"]:
            return

        if self.message_group_id:
            if callable(self.message_group_id):
                message_group_id = self.di.call_function(self.message_group_id, model=model)
                if not isinstance(message_group_id, str):
                    raise ValueError(
                        f"I called the message_group_id function for SQS for model '{model.__class__.__name__}' but the value it returned was not a string.  The message group id must be a string."
                    )
            else:
                message_group_id = self.message_group_id
            params["MessageGroupId"] = message_group_id

        client.send_message(**params)

    def get_queue_url(self, model: Model):
        if self.queue_url:
            return self.queue_url
        if self.queue_url_environment_key:
            return self.environment.get(self.queue_url_environment_key)
        return self.di.call_function(self.queue_url_callable, model=model)
