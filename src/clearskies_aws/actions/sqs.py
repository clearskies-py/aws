from __future__ import annotations

from typing import Callable

from botocore.exceptions import ClientError
from clearskies.configs import Callable as CallableConfig
from clearskies.configs import String
from clearskies.decorators import parameters_to_properties
from clearskies.model import Model
from types_boto3_sqs import SQSClient

from clearskies_aws import clients, configs

from .action_aws import ActionAws


class SQS(ActionAws[SQSClient]):
    """
    Send messages to Amazon SQS queues as a model action.

    Provides a clearskies action for sending messages to SQS queues. This action can be triggered
    by model events (like `on_change`, `on_create`, etc.) and automatically sends the model data
    or a custom message to the configured queue. Inherits all configuration from [`ActionAws`](action_aws.py).

    Configure the queue URL using a static value, environment variable, or callable. The message body
    can be customized with `message_callable` or defaults to the model's JSON representation. Supports
    FIFO queues with optional message group ID configuration.

    Example:
        Basic usage with static queue URL

        ```python
        import clearskies
        from clearskies_aws.actions import SQS
        from collections import OrderedDict


        class Order(clearskies.Model):
            def __init__(self, memory_backend, columns):
                super().__init__(memory_backend, columns)

            def columns_configuration(self):
                return OrderedDict(
                    [
                        clearskies.column_types.string(
                            "status",
                            on_change=[
                                SQS(queue_url="https://sqs.us-west-2.amazonaws.com/123/order-queue")
                            ],
                        ),
                    ]
                )
        ```

    Example:
        Using environment variable for queue URL

        ```python
        import clearskies
        from clearskies_aws.actions import SQS
        from collections import OrderedDict


        class User(clearskies.Model):
            def __init__(self, memory_backend, columns):
                super().__init__(memory_backend, columns)

            def columns_configuration(self):
                return OrderedDict(
                    [
                        clearskies.column_types.string(
                            "email",
                            on_create=[SQS(queue_url_environment_key="USER_QUEUE_URL")],
                        ),
                    ]
                )
        ```

    Example:
        FIFO queue with message group ID

        ```python
        import clearskies
        from clearskies_aws.actions import SQS
        from collections import OrderedDict


        def get_message_group_id(model):
            return f"customer-{model.customer_id}"


        class Order(clearskies.Model):
            def __init__(self, memory_backend, columns):
                super().__init__(memory_backend, columns)

            def columns_configuration(self):
                return OrderedDict(
                    [
                        clearskies.column_types.string(
                            "status",
                            on_change=[
                                SQS(
                                    queue_url="https://sqs.us-west-2.amazonaws.com/123/orders.fifo",
                                    message_group_id=get_message_group_id,
                                )
                            ],
                        ),
                    ]
                )
        ```

    Example:
        Custom message and conditional execution

        ```python
        import clearskies
        from clearskies_aws.actions import SQS
        from collections import OrderedDict
        import json


        def format_message(model):
            return json.dumps(
                {
                    "order_id": model.id,
                    "action": "send_confirmation",
                    "customer_email": model.customer_email,
                }
            )


        def only_if_paid(model):
            return model.status == "paid"


        class Order(clearskies.Model):
            def __init__(self, memory_backend, columns):
                super().__init__(memory_backend, columns)

            def columns_configuration(self):
                return OrderedDict(
                    [
                        clearskies.column_types.string(
                            "status",
                            on_change=[
                                SQS(
                                    queue_url="https://sqs.us-west-2.amazonaws.com/123/confirmations",
                                    message_callable=format_message,
                                    when=only_if_paid,
                                )
                            ],
                        ),
                    ]
                )
        ```
    """

    # Default client for SQS service
    client = configs.AwsClient(required=True, default=clients.SqsClient())

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
        message_group_id: str | Callable | None = None,
        client: clients.SqsClient | None = None,
    ) -> None:
        """Set up the SQS action."""
        self.finalize_and_validate_configuration()

    def finalize_and_validate_configuration(self):
        super().finalize_and_validate_configuration()
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

    def __call__(self, model: Model) -> None:
        """Execute SQS send message action."""
        # Check conditional execution
        if self.when and not self.di.call_function(self.when, model=model):
            return

        # Get queue URL and validate
        queue_url = self.get_queue_url(model)
        if not queue_url:
            return

        # Build message parameters
        params = {
            "QueueUrl": queue_url,
            "MessageBody": self.get_message_body(model),
        }

        # Add message group ID for FIFO queues
        if self.message_group_id:
            if callable(self.message_group_id):
                message_group_id = self.di.call_function(self.message_group_id, model=model)
                if not isinstance(message_group_id, str):
                    raise ValueError(f"Message group ID callable returned {type(message_group_id)}, expected string.")
            else:
                message_group_id = self.message_group_id
            params["MessageGroupId"] = message_group_id

        # Get client and send message
        try:
            boto3_client = self.client()
            boto3_client.send_message(**params)
        except ClientError as e:
            self.logging.exception("Failed to send SQS message.")
            raise e

    def get_queue_url(self, model: Model):
        if self.queue_url:
            return self.queue_url
        if self.queue_url_environment_key:
            return self.environment.get(self.queue_url_environment_key)
        return self.di.call_function(self.queue_url_callable, model=model)
