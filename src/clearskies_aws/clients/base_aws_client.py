"""Base AWS client class for clearskies DI."""

from __future__ import annotations

from types import ModuleType

from clearskies.configurable import Configurable
from clearskies.di.inject import ByStandardLib, Environment
from clearskies.di.injectable_properties import InjectableProperties

import clearskies_aws.di.inject.boto3
from clearskies_aws.actions.assume_role import AssumeRole as AssumeRoleAction
from clearskies_aws.configs import AssumeRole, Region


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

    service_name = ""

    boto3 = ByStandardLib("boto3")
    environment = Environment()

    """
    AWS region to use for the client.

    When not provided, region detection follows this priority:

     1. `aws_region` parameter
     2. AWS_REGION environment variable
     3. DEFAULT_AWS_REGION environment variable
     4. None (uses boto3 defaults)

    ```python
    from clearskies_aws.clients import SqsClient

    sqs = SqsClient(aws_region='us-west-2')
    client = sqs()
    client.send_message(QueueUrl='https://queue.url', MessageBody='Hello')
    ```
    """
    aws_region = Region(default="")

    """
    Optional IAM role(s) to assume before creating the client.

    Allows the client to assume a different IAM role before creating the boto3 client or resource.
    This is useful for cross-account access or when you need elevated permissions.  This accepts
    either a single clearskies_aws.actions.AssumeRole instance or a list of them, for a chain
    of assume role operations.

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
    assume_role = AssumeRole(default=[])

    """
    Whether to cache the created client or resource.

    When True (default), the client or resource is created once and returned on subsequent calls.
    When False, a new client or resource is created on every call. Set to False if you need
    to create multiple instances with different configurations.
    """
    cache: bool = True

    def __init__(
        self,
        aws_region: str | None = None,
        assume_role: AssumeRoleAction | list[AssumeRoleAction] = [],
        cache: bool = True,
    ) -> None:
        """
        Initialize the AWS client with optional configuration.

        ```python
        from clearskies_aws.clients import SqsClient
        from clearskies_aws.actions import AssumeRole

        # Basic initialization
        sqs = SqsClient(aws_region="us-west-2")

        # With role assumption and no caching
        assume_role = AssumeRole(role_arn="arn:aws:iam::123456789012:role/MyRole")
        sqs = SqsClient(aws_region="us-west-2", assume_role_config=assume_role, cache=False)
        ```
        """
        self.aws_region = aws_region if aws_region else ""
        self.assume_role = assume_role if assume_role else []
        self.cache = cache

    def get_region(self) -> str | None:
        """
        Get the AWS region to use for client creation.

        Returns the region from the first available source:
        aws_region parameter, AWS_REGION environment variable,
        DEFAULT_AWS_REGION environment variable, or None.
        """
        if self.aws_region:
            return self.aws_region

        region = self.environment.get("AWS_REGION", silent=True)
        if region:
            return region

        region = self.environment.get("DEFAULT_AWS_REGION", silent=True)
        if region:
            return region

        return None

    def create_client(
        self,
        aws_region: str | None = None,
        assume_role: list[AssumeRoleAction] | AssumeRoleAction = [],
        **kwargs,
    ):
        """
        Create a boto3 client for the specified AWS service.

        This is typically called by service-specific client classes rather than directly.
        Automatically handles region configuration and role assumption.

        ```python
        from clearskies_aws.clients import BaseAwsClient

        base_client = BaseAwsClient(aws_region="us-west-2")
        s3_client = base_client.create_client("s3")
        ```
        """
        boto3_module = self.boto3

        if assume_role or self.assume_role:
            roles = assume_role or self.assume_role
            if not isinstance(roles, list):
                roles = [roles]
            for role in roles:
                boto3_module = role(boto3_module)  # type: ignore

        region = aws_region or self.get_region()

        if region:
            return boto3_module.client(self.service_name, region_name=region, **kwargs)

        return boto3_module.client(self.service_name, **kwargs)
