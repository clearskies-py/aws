from __future__ import annotations

from types import ModuleType


class AssumeRole:
    """
    Assume an IAM role before making AWS calls.

    Used by AWS actions and clients to assume a different IAM role before creating boto3 clients.
    This enables cross-account access, elevated permissions, or role chaining. You configure
    this once and pass it to any AWS action or client that needs it.

    Note that this class assumes you already have AWS credentials properly configured and findable
    by boto3 in the standard way. If you're trying to use static IAM credentials, that's possible
    with some undocumented hackery but not the intended use case.

    Example:
        Basic usage with an SQS action on a model trigger

        ```python
        import clearskies
        from clearskies_aws.actions import SQS, AssumeRole
        from collections import OrderedDict


        class User(clearskies.Model):
            def __init__(self, memory_backend, columns):
                super().__init__(memory_backend, columns)

            def columns_configuration(self):
                return OrderedDict(
                    [
                        clearskies.column_types.string(
                            "name",
                            on_change=[
                                SQS(
                                    queue_url="https://queue.url.example.aws.com",
                                    assume_role=AssumeRole(
                                        role_arn="arn:aws:iam:role/name",
                                        external_id="12345",
                                    ),
                                )
                            ],
                        ),
                    ]
                )
        ```

    Example:
        Role chaining with multiple assume operations

        ```python
        from clearskies_aws.actions import SQS, AssumeRole
        from collections import OrderedDict
        import clearskies

        first_assume_role = AssumeRole(
            role_arn="arn:aws:123456789012:iam:role/name",
            external_id="12345",
        )
        final_assume_role = AssumeRole(
            role_arn="arn:aws:210987654321:iam:role/name-2",
            external_id="54321",
            source=first_assume_role,
        )


        class User(clearskies.Model):
            def __init__(self, memory_backend, columns):
                super().__init__(memory_backend, columns)

            def columns_configuration(self):
                return OrderedDict(
                    [
                        clearskies.column_types.string(
                            "name",
                            on_change=[
                                SQS(
                                    queue_url="https://queue.url.example.aws.com",
                                    assume_role=final_assume_role,
                                )
                            ],
                        ),
                    ]
                )
        ```
    """

    """The ARN of the role to assume"""
    role_arn = ""

    """Optional external ID for enhanced security"""
    external_id = ""

    """Session name for the assumed role session (defaults to 'clearkies-aws')"""
    role_session_name = ""

    """Duration of the assumed role session in seconds (default: 3600)"""
    duration = 3600

    """Optional role to assume first (for role chaining)"""
    source: AssumeRole | None = None

    def __init__(
        self,
        role_arn: str,
        external_id: str = "",
        role_session_name: str = "",
        duration: int = 3600,
        source: AssumeRole | None = None,
    ):
        """
        Configure the role assumption.

        The role_arn is the only required parameter. All other parameters are optional
        and provide additional configuration for the assume role operation.
        """
        self.role_arn = role_arn
        self.external_id = external_id
        self.role_session_name = role_session_name
        self.duration = duration
        self.source = source

    def __call__(self, boto3: ModuleType) -> ModuleType:
        """
        Assume the configured role and return a new boto3 session.

        This is called internally by AWS actions and clients. It takes a boto3 module/session,
        assumes the configured role, and returns a new boto3 session with the assumed credentials.

        Support role chaining by checking for a source role first.
        """
        # chaining!
        if self.source:
            boto3 = self.source(boto3)

        calling_params = {
            "RoleArn": self.role_arn,
            "RoleSessionName": self.role_session_name if self.role_session_name else "clearkies-aws",
            "DurationSeconds": self.duration,
        }
        if self.external_id:
            calling_params["ExternalId"] = self.external_id
        credentials = boto3.client("sts").assume_role(**calling_params)["Credentials"]

        # now let's make a new session using those
        return boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )
