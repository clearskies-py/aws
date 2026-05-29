from __future__ import annotations

from typing import TYPE_CHECKING, Any

from clearskies.di.injectable import Injectable

if TYPE_CHECKING:
    from clearskies_aws.clients import BaseAwsClient


class Client(Injectable):
    """
    Base class to inject a boto3 client.

    The extending class must have an attribute named `client_class` which points to a class in
    clearskies_aws.clients that will be responsible for actually building the client, if needed.

    This assumes that the class using the injectable has three configs defined:

    |      Name               |             Config Class            |
    |:-----------------------:|:-----------------------------------:|
    | `client_injection_name` |  `clearskies.configs.Environment`   |
    | `aws_region`            |   `clearskies_aws.configs.Region`   |
    | `assume_role`           | `clearskies_aws.configs.AssumeRole` |

    This class will preferably fetch the client from the DI system based on `client_injection_name` or,
    if not present, it will generate boto3 clients for the given region/assume role configuration.
    If `aws_region` is not defined then it will fall back on the `AWS_REGION` and `AWS_DEFAULT_REGION`
    environment variables (in that order).
    """

    client: BaseAwsClient
    _boto3_client: Any

    @property
    def client_class(self) -> type[BaseAwsClient]:
        raise NotImplementedError()

    def build_client(self, instance: Any) -> Any:
        if hasattr(self, "_boto3_client") and self._boto3_client:
            return self._boto3_client

        if hasattr(instance, "client_injection_name") and instance.client_injection_name:
            self._boto3_client = self._di.build_from_name(instance.client_injection_name)
            return self._boto3_client

        self._di.inject_properties(self.client_class)
        client = self.client_class(aws_region=instance.aws_region, assume_role=instance.assume_role)
        self._boto3_client = client.create_client()
        return self._boto3_client

    def set_di(self, di):
        self._di = di
        self._boto3_client = None
