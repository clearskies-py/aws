from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

from botocore.exceptions import ClientError as BotoClientError
from clearskies import Endpoint, exceptions
from clearskies.configs import Callable as CallableConfig
from clearskies.configs import Schema as SchemaConfig
from clearskies.configs import StringList
from clearskies.decorators import parameters_to_properties
from clearskies.di.inject import Di
from clearskies.exceptions import ClientError
from clearskies.input_outputs import InputOutput

from clearskies_aws.di import inject


class SecretsManagerRotation(Endpoint):
    di = Di()
    boto3 = inject.Boto3()

    current = "AWSCURRENT"
    pending = "AWSPENDING"

    steps = StringList(default=["createSecret", "setSecret", "testSecret", "finishSecret"])
    create_secret = CallableConfig(default=None)
    set_secret = CallableConfig(default=None)
    test_secret = CallableConfig(default=None)
    finish_secret = CallableConfig(default=None)
    schema = SchemaConfig(default=None)

    @parameters_to_properties
    def __init__(
        self,
        steps: list[str] | None = None,
        create_secret: Callable | None = None,
        set_secret: Callable | None = None,
        test_secret: Callable | None = None,
        finish_secret: Callable | None = None,
        schema: list[dict[str, Any]] | None = None,
    ) -> None:
        super().__init__()

    def configure(self) -> None:
        self.finalize_and_validate_configuration()
        class_name = self.__class__.__name__
        if not self.create_secret:
            raise KeyError(f"Missing required configuration 'createSecret' for handler {class_name}")

        allowed_steps = {"createSecret", "setSecret", "testSecret", "finishSecret"}
        for step in self.steps:
            if step not in allowed_steps:
                raise KeyError(
                    f"Invalid configured step '{step}' for handler {class_name}. Allowed steps: {sorted(allowed_steps)}"
                )

        for config in [self.create_secret, self.set_secret, self.test_secret, self.finish_secret]:
            if config is not None and not callable(config):
                raise TypeError("Configured rotation step handlers must be callables")

    def _parse_request_data(self, input_output: InputOutput) -> dict[str, Any]:
        request_data = json.loads(input_output.get_body())
        if not isinstance(request_data, dict):
            raise ClientError("Invalid payload: request body must be a JSON object")
        return request_data

    def _validate_secret_data(self, secret_data: dict[str, Any], input_output: InputOutput, label: str) -> None:
        if not self.schema:
            return
        try:
            self.find_input_errors(secret_data, input_output, self.schema)
        except exceptions.InputErrors as error:
            raise ValueError(f"The {label} did not match the configured schema: {error}") from error

    def _load_secret_json(self, secret_response: dict[str, Any], label: str) -> dict[str, Any]:
        secret_string = secret_response.get("SecretString")
        if not isinstance(secret_string, str):
            raise ValueError(f"The {label} is missing a valid SecretString")
        parsed = json.loads(secret_string)
        if not isinstance(parsed, dict):
            raise ValueError(f"The {label} must be a JSON object")
        return parsed

    def handle(self, input_output: InputOutput) -> None:
        request_data = self._parse_request_data(input_output)

        if self.schema:
            self.find_input_errors(request_data, input_output, self.schema)

        arn = request_data.get("SecretId")
        request_token = request_data.get("ClientRequestToken")
        step = request_data.get("Step")
        if not isinstance(arn, str) or not arn:
            raise ClientError("Invalid payload: SecretId is required and must be a string")
        if not isinstance(request_token, str) or not request_token:
            raise ClientError("Invalid payload: ClientRequestToken is required and must be a string")
        if not isinstance(step, str) or not step:
            raise ClientError("Invalid payload: Step is required and must be a string")

        secretsmanager = self.boto3.client("secretsmanager")
        metadata = secretsmanager.describe_secret(SecretId=arn)

        self._validate_secret_and_request(step, arn, metadata, request_token)

        current_secret = secretsmanager.get_secret_value(SecretId=arn, VersionStage=self.current)
        current_secret_data = self._load_secret_json(current_secret, "current secret")
        self._validate_secret_data(current_secret_data, input_output, "current secret")

        pending_secret_data: dict[str, Any] = {}

        # check for a pending secret.  Note that this is not always available.  In the event that we are retrying a failed
        # rotation it will already be set, in which case we need to skip the createSecret step.
        try:
            pending_secret = secretsmanager.get_secret_value(
                SecretId=arn, VersionId=request_token, VersionStage=self.pending
            )
            pending_secret_data = self._load_secret_json(pending_secret, "pending secret")
        except BotoClientError as error:
            if error.response["Error"]["Code"] == "ResourceNotFoundException":
                pending_secret_data = {}
            else:
                raise

        # we can't call the createSecret step if we already have a pending secret or this will generate an error from AWS.
        if step == "createSecret" and pending_secret_data:
            return

        # call the appropriate step and pass along *everything*.
        getattr(self, step)(
            current_secret_data=current_secret_data,
            pending_secret_data=pending_secret_data,
            secretsmanager=secretsmanager,
            metadata=metadata,
            request_token=request_token,
            arn=arn,
            input_output=input_output,
        )

    def _validate_secret_and_request(self, step: str, arn: str, metadata: dict[str, Any], request_token: str) -> None:
        """Perform basic checks suggested by AWS of both the request and the secret to ensure validity."""
        if step not in self.steps:
            raise ClientError(f"Invalid step: {step}")

        if not metadata.get("RotationEnabled"):
            raise ValueError("Secret %s is not enabled for rotation" % arn)

        versions = metadata["VersionIdsToStages"]
        prefix = f"Rotation config error for version '{request_token}' of secret '{arn}': "
        if request_token not in versions:
            raise ValueError(f"{prefix} we don't have a stage for rotation")
        if self.current in versions[request_token]:
            raise ValueError(
                f"{prefix} it's already the current version, which shouldn't happen.  I'm quitting with prejudice."
            )
        elif self.pending not in versions[request_token]:
            raise ValueError(f"{prefix} it hasn't been set to pending yet, which makes no sense!")

    def createSecret(self, **kwargs) -> None:
        if not self.create_secret:
            return

        new_secret_data = self.di.call_function(self.create_secret, **kwargs)
        if new_secret_data is None:
            raise ValueError(
                f"I called the configured createSecret function but it didn't return anything.  It has to return the new secret data."
            )
        if not isinstance(new_secret_data, dict):
            raise ValueError(
                f"I called the configured createSecret function but it didn't return a dictionary.  The createSecret function must return a dictionary."
            )

        input_output = kwargs["input_output"]
        self._validate_secret_data(new_secret_data, input_output, "secret data returned by createSecret")

        # if we get this far we can store the new data
        secretsmanager = kwargs["secretsmanager"]
        request_token = kwargs["request_token"]
        arn = kwargs["arn"]
        secretsmanager.put_secret_value(
            SecretId=arn,
            SecretString=json.dumps(new_secret_data),
            ClientRequestToken=request_token,
            VersionStages=[self.pending],
        )

    def setSecret(self, **kwargs) -> None:
        if not self.set_secret:
            return
        self.di.call_function(self.set_secret, **kwargs)

    def testSecret(self, **kwargs) -> None:
        if not self.test_secret:
            return
        self.di.call_function(self.test_secret, **kwargs)

    def finishSecret(self, **kwargs) -> None:
        if self.finish_secret:
            self.di.call_function(self.finish_secret, **kwargs)

        secretsmanager = kwargs["secretsmanager"]
        request_token = kwargs["request_token"]
        arn = kwargs["arn"]
        metadata = kwargs["metadata"]
        current_version = None
        for version in metadata["VersionIdsToStages"]:
            if self.current not in metadata["VersionIdsToStages"][version]:
                continue

            if version == request_token:
                return

            current_version = version
            break

        # finish the rotation by taking the new version and making it current.
        secretsmanager.update_secret_version_stage(
            SecretId=arn, VersionStage=self.current, MoveToVersionId=request_token, RemoveFromVersionId=current_version
        )
