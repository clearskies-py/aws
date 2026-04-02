"""Injectable for SES client."""

from __future__ import annotations

from clearskies.di.injectable import Injectable
from types_boto3_ses import SESClient as Boto3SESClient

if TYPE_CHECKING:
    from clearskies_aws.clients import BaseAwsClient

class Ses(Injectable):
    """
    Injectable wrapper for SES client.

    This injectable provides access to a boto3 SES client instance that is
    configured through the clearskies DI system.

    Usage::

        from clearskies_aws.di import inject


        class MyAction(InjectableProperties):
            ses = inject.SesClient()

            def send_email(self):
                self.ses.send_email(
                    Source="sender@example.com",
                    Destination={"ToAddresses": ["recipient@example.com"]},
                    Message={...},
                )
    """

    @property
    def client_class(self) -> type[BaseAwsClient]:
        from clearskies_aws.clients import SesClient
        return SesClient

    client_class = SesClient

    def __get__(self, instance, parent) -> Boto3SESClient:
        """
        Get the SES client from the DI container.

        Returns:
            Boto3 SES client instance
        """
        if parent is None:
            return instance # type: ignore

        return self.build_client()  # type: ignore
