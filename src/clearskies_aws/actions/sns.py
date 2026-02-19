from __future__ import annotations

from typing import Callable

from botocore.exceptions import ClientError
from clearskies import Model
from clearskies.configs import Callable as CallableConfig
from clearskies.configs import String
from clearskies.decorators import parameters_to_properties
from types_boto3_sns import SNSClient

from clearskies_aws import clients, configs

from .action_aws import ActionAws


class SNS(ActionAws[SNSClient]):
    """
    Publish messages to Amazon SNS topics as a model action.

    Provides a clearskies action for publishing messages to SNS topics. This action can be triggered
    by model events (like `on_change`, `on_create`, etc.) and automatically publishes the model data
    or a custom message to the configured topic. Inherits all configuration from [`ActionAws`](action_aws.py).

    Configure the topic ARN using a static value, environment variable, or callable. The message body
    can be customized with `message_callable` or defaults to the model's JSON representation.

    Example:
        Basic usage with static topic

        ```python
        import clearskies
        from clearskies_aws.actions import SNS
        from collections import OrderedDict


        class Order(clearskies.Model):
            def __init__(self, memory_backend, columns):
                super().__init__(memory_backend, columns)

            def columns_configuration(self):
                return OrderedDict(
                    [
                        clearskies.column_types.string(
                            "status",
                            on_change=[SNS(topic="arn:aws:sns:us-west-2:123456789012:order-updates")],
                        ),
                    ]
                )
        ```

    Example:
        Using environment variable for topic

        ```python
        import clearskies
        from clearskies_aws.actions import SNS
        from collections import OrderedDict


        class User(clearskies.Model):
            def __init__(self, memory_backend, columns):
                super().__init__(memory_backend, columns)

            def columns_configuration(self):
                return OrderedDict(
                    [
                        clearskies.column_types.string(
                            "email",
                            on_create=[SNS(topic_environment_key="USER_CREATED_TOPIC_ARN")],
                        ),
                    ]
                )
        ```

    Example:
        Custom message with conditional execution

        ```python
        import clearskies
        from clearskies_aws.actions import SNS
        from collections import OrderedDict
        import json


        def format_order_message(model):
            return json.dumps(
                {
                    "order_id": model.id,
                    "status": model.status,
                    "total": float(model.total),
                    "customer_email": model.customer_email,
                }
            )


        def only_if_completed(model):
            return model.status == "completed"


        class Order(clearskies.Model):
            def __init__(self, memory_backend, columns):
                super().__init__(memory_backend, columns)

            def columns_configuration(self):
                return OrderedDict(
                    [
                        clearskies.column_types.string(
                            "status",
                            on_change=[
                                SNS(
                                    topic="arn:aws:sns:us-west-2:123:orders",
                                    message_callable=format_order_message,
                                    when=only_if_completed,
                                )
                            ],
                        ),
                    ]
                )
        ```
    """

    # Default client for SNS service
    client = configs.AwsClient(required=True, default=clients.SnsClient())

    topic = String(required=False)
    topic_environment_key = String(required=False)
    topic_callable = CallableConfig(required=False)

    @parameters_to_properties
    def __init__(
        self,
        topic: str | None = None,
        topic_environment_key: str | None = None,
        topic_callable: Callable | None = None,
        message_callable: Callable | None = None,
        when: Callable | None = None,
        client: clients.SnsClient | None = None,
    ) -> None:
        """Configure the SNS action."""
        self.finalize_and_validate_configuration()

    def finalize_and_validate_configuration(self):
        super().finalize_and_validate_configuration()
        topics = 0
        for value in [self.topic, self.topic_environment_key, self.topic_callable]:
            if value:
                topics += 1
        if topics > 1:
            raise ValueError(
                "You can only provide one of 'topic', 'topic_environment_key', or 'topic_callable', but more than one were provided."
            )
        if not topics:
            raise ValueError("You must provide at least one of 'topic', 'topic_environment_key', or 'topic_callable'.")

    def __call__(self, model: Model) -> None:
        """Execute SNS publish action."""
        # Check conditional execution
        if self.when and not self.di.call_function(self.when, model=model):
            return

        # Get topic ARN and validate
        topic_arn = self.get_topic_arn(model)
        if not topic_arn:
            return

        # Get client and publish
        try:
            boto3_client = self.client()
            boto3_client.publish(
                TopicArn=topic_arn,
                Message=self.get_message_body(model),
            )
        except ClientError as e:
            self.logging.exception("Failed to publish to SNS topic.")
            raise e

    def get_topic_arn(self, model: Model) -> str:
        if self.topic:
            return self.topic
        if self.topic_environment_key:
            return self.environment.get(self.topic_environment_key)
        return self.di.call_function(self.topic_callable, model=model)
