from __future__ import annotations

import datetime
import json
from collections.abc import Sequence
from types import ModuleType
from typing import Callable, cast

from botocore.exceptions import ClientError
from clearskies import Model
from clearskies.decorators import parameters_to_properties
from clearskies.environment import Environment

from .action_aws import ActionAws
from .assume_role import AssumeRole


class SNS(ActionAws):

    @parameters_to_properties
    def __init__(
        self,
        topic=None,
        topic_environment_key=None,
        topic_callable: Callable | None = None,
        message_callable: Callable | None = None,
        when: Callable | None = None,
        assume_role: AssumeRole | None = None,
    ) -> None:
        """Configure the SNS action."""
        super().__init__(service_name="sns", message_callable=message_callable, when=when, assume_role=assume_role)
        self.finalize_and_validate_configuration()

        self.topic = topic
        self.topic_environment_key = topic_environment_key
        self.topic_callable = topic_callable

        topics = 0
        for value in [topic, topic_environment_key, topic_callable]:
            if value:
                topics += 1
        if topics > 1:
            raise ValueError(
                "You can only provide one of 'topic', 'topic_environment_key', or 'topic_callable', but more than one were provided."
            )
        if not topics:
            raise ValueError("You must provide at least one of 'topic', 'topic_environment_key', or 'topic_callable'.")

    def _execute_action(self, client: ModuleType, model: Model) -> None:
        """Send a notification as configured."""
        topic_arn = self.get_topic_arn(model)
        if not topic_arn:
            return
        client.publish(
            TopicArn=self.get_topic_arn(model),
            Message=self.get_message_body(model),
        )

    def get_topic_arn(self, model: Model) -> str:
        if self.topic:
            return self.topic
        if self.topic_environment_key:
            return self.environment.get(self.topic_environment_key)
        return self.di.call_function(self.topic_callable, model=model)
