from __future__ import annotations

import datetime
from typing import Any, Callable

import clearskies
import jinja2
from botocore.exceptions import ClientError
from clearskies import Model
from clearskies.configs import Any as AnyConfig
from clearskies.configs import Email, EmailOrEmailListOrCallable, String
from clearskies.decorators import parameters_to_properties
from types_boto3_ses import SESClient

from clearskies_aws import clients, configs
from clearskies_aws.actions import action_aws


class SES(action_aws.ActionAws[SESClient]):
    """
    Send emails via Amazon SES as a model action.

    Provides a clearskies action for sending emails through Amazon SES. This action can be triggered
    by model events (like`on_change`, `on_create`, etc.) and supports both static and templated content
    using Jinja2. Inherits all configuration from [`ActionAws`](action_aws.py).

    Configure email recipients, subject, and body using either static values or templates. You can also
    use callables to dynamically determine recipients based on the model being acted upon.

    Example:
        Basic usage with static content

        ```python
        import clearskies
        from clearskies_aws.actions import SES
        from collections import OrderedDict


        class User(clearskies.Model):
            def __init__(self, memory_backend, columns):
                super().__init__(memory_backend, columns)

            def columns_configuration(self):
                return OrderedDict(
                    [
                        clearskies.column_types.string(
                            "email",
                            on_create=[
                                SES(
                                    sender="noreply@example.com",
                                    to="email",  # uses the email column from the model
                                    subject="Welcome!",
                                    message="Thank you for signing up.",
                                )
                            ],
                        ),
                    ]
                )
        ```

    Example:
        Using Jinja2 templates

        ```python
        import clearskies
        from clearskies_aws.actions import SES
        import jinja2
        from collections import OrderedDict

        welcome_template = jinja2.Template(\"\"\"
            <h1>Welcome {{model.name}}!</h1>
            <p>Your account was created on {{now.strftime('%Y-%m-%d')}}.</p>
        \"\"\")

        class User(clearskies.Model):
            def __init__(self, memory_backend, columns):
                super().__init__(memory_backend, columns)

            def columns_configuration(self):
                return OrderedDict([
                    clearskies.column_types.string(
                        "name",
                        on_create=[
                            SES(
                                sender="noreply@example.com",
                                to="email",
                                subject="Welcome {{model.name}}!",
                                message_template=welcome_template
                            )
                        ],
                    ),
                ])
        ```

    Example:
        Dynamic recipients with callables

        ```python
        import clearskies
        from clearskies_aws.actions import SES
        from collections import OrderedDict


        def get_admin_emails(model):
            # Could fetch from database, config, etc.
            return ["admin1@example.com", "admin2@example.com"]


        class Order(clearskies.Model):
            def __init__(self, memory_backend, columns):
                super().__init__(memory_backend, columns)

            def columns_configuration(self):
                return OrderedDict(
                    [
                        clearskies.column_types.string(
                            "status",
                            on_change=[
                                SES(
                                    sender="orders@example.com",
                                    to=get_admin_emails,
                                    subject="Order Status Changed",
                                    message_callable=lambda model: (
                                        f"Order {model.id} status: {model.status}"
                                    ),
                                )
                            ],
                        ),
                    ]
                )
        ```
    """

    # Default client for SES service
    client = configs.AwsClient(required=True, default=clients.SesClient())

    sender = Email(required=True)
    to = EmailOrEmailListOrCallable(required=False)
    cc = EmailOrEmailListOrCallable(required=False)
    bcc = EmailOrEmailListOrCallable(required=False)
    subject = String(required=False)
    message = String(required=False)
    subject_template = AnyConfig(required=False)
    message_template = AnyConfig(required=False)
    subject_template_file = String(required=False)
    message_template_file = String(required=False)
    dependencies_for_template: list[Any] = []

    destinations: dict[str, list[str | Callable]] = {
        "to": [],
        "cc": [],
        "bcc": [],
    }

    @parameters_to_properties
    def __init__(
        self,
        sender: str,
        to: list | str | Callable | None = None,
        cc: list | str | Callable | None = None,
        bcc: list | str | Callable | None = None,
        subject: str | None = None,
        message: str | None = None,
        subject_template: jinja2.Template | None = None,
        message_template: jinja2.Template | None = None,
        subject_template_file: str | None = None,
        message_template_file: str | None = None,
        dependencies_for_template: list[Any] = [],
        when: Callable | None = None,
        client: clients.SesClient | None = None,
    ) -> None:
        """Configure the rules for this email notification."""
        self.finalize_and_validate_configuration()

    def finalize_and_validate_configuration(self):
        # First finalize and validate configuration to set up defaults

        # this just moves the data from the various "to" inputs (to, cc, bcc) into the self.destinations
        # dictionary, after normalizing it so that it is always a list.
        super().finalize_and_validate_configuration()
        if not self.to and not self.cc and not self.bcc:
            raise ValueError("You must configure at least one 'to' address or one 'cc' address or one 'bcc' address")

        for key in self.destinations.keys():
            destination_values = getattr(self, key, None)
            if not destination_values:
                continue
            if type(destination_values) == str or callable(destination_values):
                self.destinations[key] = [destination_values]
            else:
                self.destinations[key] = destination_values
        num_subjects = 0
        num_messages = 0
        for source in [self.subject, self.subject_template, self.subject_template_file]:
            if source:
                num_subjects += 1
        for source in [self.message, self.message_template, self.message_template_file]:
            if source:
                num_messages += 1
        if num_subjects > 1:
            raise ValueError(
                "More than one of 'subject', 'subject_template', or 'subject_template_file' was set, but only one of these may be set."
            )
        if num_messages > 1:
            raise ValueError(
                "More than one of 'message', 'message_template', or 'message_template_file' was set, but only one of these may be set."
            )

        if self.subject_template_file:
            with open(self.subject_template_file, "r", encoding="utf-8") as template:
                self.subject_template = jinja2.Template(template.read())
        elif self.subject_template and not isinstance(self.subject_template, jinja2.Template):
            self.subject_template = jinja2.Template(self.subject_template)

        if self.message_template_file:
            with open(self.message_template_file, "r", encoding="utf-8") as template:
                self.message_template = jinja2.Template(template.read())
        elif self.message_template and not isinstance(self.message_template, jinja2.Template):
            self.message_template = jinja2.Template(self.message_template)

    def __call__(self, model: Model) -> None:
        """Send a notification as configured."""
        utcnow = self.di.build("utcnow")

        tos = self.resolve_destination("to", model)
        if not tos:
            return
        response = self.client().send_email(
            Destination={
                "ToAddresses": tos,
                "CcAddresses": self.resolve_destination("cc", model),
                "BccAddresses": self.resolve_destination("bcc", model),
            },
            Message={
                "Body": {
                    "Html": {
                        "Charset": "utf-8",
                        "Data": self.resolve_message_as_html(model, utcnow),
                    },
                    "Text": {
                        "Charset": "utf-8",
                        "Data": self.resolve_message_as_text(model, utcnow),
                    },
                },
                "Subject": {"Charset": "utf-8", "Data": self.resolve_subject(model, utcnow)},
            },
            Source=self.sender,
        )

    def resolve_destination(self, name: str, model: clearskies.Model) -> list[str]:
        """
        Return a list of to/cc/bcc addresses.

        Each entry can be:

         1. An email address
         2. The name of a column in the model that contains an email address
        """
        resolved = []
        destinations = self.destinations[name]
        for destination in destinations:
            if callable(destination):
                more = self.di.call_function(destination, model=model)
                if not isinstance(more, list):
                    more = [more]
                for entry in more:
                    if not isinstance(entry, str):
                        raise ValueError(
                            f"I invoked a callable to fetch the '{name}' addresses for model '{model.__class__.__name__}' but it returned something other than a string.  Callables must return a valid email address or a list of email addresses."
                        )
                    if "@" not in entry:
                        raise ValueError(
                            f"I invoked a callable to fetch the '{name}' addresses for model '{model.__class__.__name__}' but it returned a non-email address.  Callables must return a valid email address or a list of email addresses."
                        )
                resolved.extend(more)
                continue
            if "@" in destination:
                resolved.append(destination)
                continue
            resolved.append(getattr(model, destination))
        return resolved

    def resolve_message_as_html(self, model: clearskies.Model, now: datetime.datetime) -> str:
        """Build the HTML for a message."""
        if self.message:
            return self.message

        if self.message_template:
            return str(
                self.message_template.render(model=model, now=now, **self.more_template_variables(), text_in_html=True)
            )

        return ""

    def resolve_message_as_text(self, model: clearskies.Model, now: datetime.datetime) -> str:
        """Build the text for a message."""
        if self.message:
            return self.message

        if self.message_template:
            return str(self.message_template.render(model=model, now=now, **self.more_template_variables()))

        return ""

    def resolve_subject(self, model: clearskies.Model, now: datetime.datetime) -> str:
        """Build the subject for a message."""
        if self.subject:
            return self.subject

        if self.subject_template:
            return str(self.subject_template.render(model=model, now=now, **self.more_template_variables()))

        return ""

    def more_template_variables(self) -> dict[str, Any]:
        more_variables = {}
        for dependency_name in self.dependencies_for_template:
            more_variables[dependency_name] = self.di.build(dependency_name, cache=True)
        return more_variables
