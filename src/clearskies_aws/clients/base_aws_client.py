"""Base AWS client class for clearskies DI."""

from __future__ import annotations

from types import ModuleType

from clearskies.configurable import Configurable
from clearskies.di.inject import Environment
from clearskies.di.injectable_properties import InjectableProperties

from clearskies_aws.actions.assume_role import AssumeRole
from clearskies_aws.di import inject as aws_inject


class BaseAwsClient(Configurable, InjectableProperties):
    """
    Automating drudgery with AWS.

    Base class for AWS service clients in clearskies dependency injection. This provides common
    functionality for creating boto3 clients and resources with support for region configuration
    and IAM role assumption. All service-specific client classes inherit from this base class.

    You typically won't use this class directly, but instead use one of the service-specific
    client classes like [`SqsClient`](sqs_client.py), [`SnsClient`](sns_client.py), etc.
    These inherit all configuration options and behavior from this base class.
    """

    boto3 = aws_inject.Boto3()
    environment = Environment()

    """
    AWS region to use for the client.

    When not provided, region detection follows this priority:

     1. `region_name` parameter
     2. AWS_REGION environment variable
     3. DEFAULT_AWS_REGION environment variable
     4. None (uses boto3 defaults)

    ```python
    from clearskies_aws.clients import SqsClient

    sqs = SqsClient(region_name='us-west-2')
    client = sqs()
    client.send_message(QueueUrl='https://queue.url', MessageBody='Hello')
    ```
    """

    region_name: str | None = None

    """
    Optional IAM role to assume before creating the client.

    Allows the client to assume a different IAM role before creating the boto3 client or resource.
    This is useful for cross-account access or when you need elevated permissions.

    ```python
    from clearskies_aws.clients import SnsClient
    from clearskies_aws.actions import AssumeRole

    assume_role = AssumeRole(
        role_arn='arn:aws:iam::123456789012:role/MyRole',
        external_id='secret123'
    )
    sns = SnsClient(assume_role_config=assume_role)
    client = sns()
    client.publish(TopicArn='arn:aws:sns:us-west-2:123:topic', Message='Hello')
    ```
    """
    assume_role_config: AssumeRole | None = None

    """
    Whether to cache the created client or resource.

    When True (default), the client or resource is created once and returned on subsequent calls.
    When False, a new client or resource is created on every call. Set to False if you need
    to create multiple instances with different configurations.
    """
    cache: bool = True

    cached_client: object | None = None

    def __init__(
        self,
        region_name: str | None = None,
        assume_role_config: AssumeRole | None = None,
        cache: bool = True,
    ) -> None:
        """
        Initialize the AWS client with optional configuration.

        ```python
        from clearskies_aws.clients import SqsClient
        from clearskies_aws.actions import AssumeRole

        # Basic initialization
        sqs = SqsClient(region_name="us-west-2")

        # With role assumption and no caching
        assume_role = AssumeRole(role_arn="arn:aws:iam::123456789012:role/MyRole")
        sqs = SqsClient(region_name="us-west-2", assume_role_config=assume_role, cache=False)
        ```
        """
        self.region_name = region_name
        self.assume_role_config = assume_role_config
        self.cache = cache
        self.cached_client = None

    def get_region(self) -> str | None:
        """
        Get the AWS region to use for client creation.

        Returns the region from the first available source:
        region_name parameter, AWS_REGION environment variable,
        DEFAULT_AWS_REGION environment variable, or None.
        """
        if self.region_name:
            return self.region_name

        region = self.environment.get("AWS_REGION", silent=True)
        if region:
            return region

        region = self.environment.get("DEFAULT_AWS_REGION", silent=True)
        if region:
            return region

        return None

    def create_client(
        self,
        service_name: str,
        region_name: str | None = None,
        assume_role: AssumeRole | None = None,
        **kwargs,
    ):
        """
        Create a boto3 client for the specified AWS service.

        This is typically called by service-specific client classes rather than directly.
        Automatically handles region configuration and role assumption.

        ```python
        from clearskies_aws.clients import BaseAwsClient

        base_client = BaseAwsClient(region_name="us-west-2")
        s3_client = base_client.create_client("s3")
        ```
        """
        boto3_module = self.boto3

        if assume_role or self.assume_role_config:
            role = assume_role or self.assume_role_config
            boto3_module = role(boto3_module)  # type: ignore

        region = region_name or self.get_region()

        if region:
            return boto3_module.client(service_name, region_name=region, **kwargs)

        return boto3_module.client(service_name, **kwargs)

    def create_resource(
        self,
        service_name: str,
        region_name: str | None = None,
        assume_role: AssumeRole | None = None,
        **kwargs,
    ):
        """
        Create a boto3 resource for the specified AWS service.

        This is typically called by service-specific resource classes rather than directly.
        Automatically handles region configuration and role assumption.

        ```python
        from clearskies_aws.clients import BaseAwsClient

        base_client = BaseAwsClient(region_name="us-west-2")
        dynamodb_resource = base_client.create_resource("dynamodb")
        ```
        """
        boto3_module: ModuleType = self.boto3

        if assume_role or self.assume_role_config:
            role = assume_role or self.assume_role_config
            boto3_module = role(boto3_module)  # type: ignore

        region = region_name or self.get_region()

        if region:
            return boto3_module.resource(service_name, region_name=region, **kwargs)

        return boto3_module.resource(service_name, **kwargs)
