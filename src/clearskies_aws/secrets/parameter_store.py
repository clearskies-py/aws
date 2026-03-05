from __future__ import annotations

import re
from typing import Any

from botocore.config import Config
from botocore.exceptions import ClientError
from clearskies.exceptions.not_found import NotFound
from types_boto3_ssm import SSMClient

from clearskies_aws.secrets import secrets


class ParameterStore(secrets.Secrets[SSMClient]):
    """
    Backend for managing secrets using AWS Systems Manager Parameter Store.

    This class provides integration with AWS SSM Parameter Store, allowing you to store,
    retrieve, update, and delete secrets. All values are stored as SecureString by default
    for security.

    Paths are automatically sanitized to comply with SSM parameter naming requirements.
    AWS SSM parameter paths only allow: a-z, A-Z, 0-9, -, _, ., /, @, and :
    Any disallowed characters in the path are replaced with hyphens.

    The client is configured with adaptive retry mode which automatically handles
    throttling exceptions with exponential backoff (up to 10 retries).

    ### Example Usage

    ```python
    from clearskies_aws.secrets import ParameterStore

    secrets = ParameterStore()

    # Create/update a secret (stored as SecureString)
    secrets.update("/my-app/database-password", "super-secret")

    # Get a secret
    password = secrets.get("/my-app/database-password")

    # Delete a secret
    secrets.delete("/my-app/database-password")
    ```
    """

    ssm: SSMClient

    def __init__(self):
        """Initialize the Parameter Store backend."""
        super().__init__()

    def _sanitize_path(self, path: str) -> str:
        """
        Sanitize a secret path for use as an SSM parameter name.

        AWS SSM parameter paths only allow a-z, A-Z, 0-9, -, _, ., /, @, and :
        Any disallowed characters are replaced with hyphens.
        """
        return re.sub(r"[^a-zA-Z0-9\-_\./@:]", "-", path)

    @property
    def boto3_client(self) -> SSMClient:
        """
        Return the boto3 SSM client.

        Creates a new client if one doesn't exist yet, using the AWS_REGION environment variable.
        Configured with adaptive retry mode for better throttling handling.
        """
        if hasattr(self, "ssm"):
            return self.ssm

        # Configure adaptive retry mode with increased max attempts for throttling
        # Adaptive mode automatically adjusts retry behavior based on error responses
        # and includes exponential backoff with jitter
        retry_config = Config(
            retries={
                "max_attempts": 10,
                "mode": "adaptive",
            }
        )

        self.ssm = self.boto3.client(
            "ssm",
            region_name=self.environment.get("AWS_REGION"),
            config=retry_config,
        )
        return self.ssm

    def create(self, path: str, value: str) -> bool:
        """
        Create a new parameter in Parameter Store.

        This is an alias for update() since Parameter Store uses upsert semantics.
        """
        return self.update(path, value)

    def get(self, path: str, silent_if_not_found: bool = False) -> str | None:  # type: ignore[override]
        """
        Retrieve a parameter value from Parameter Store.

        Returns the decrypted parameter value for the given path. If silent_if_not_found
        is True, returns None when the parameter is not found instead of raising NotFound.

        Throttling is handled automatically by boto3's adaptive retry mode configured
        on the client (up to 10 retries with exponential backoff and jitter).
        """
        sanitized_path = self._sanitize_path(path)
        try:
            result = self.boto3_client.get_parameter(Name=sanitized_path, WithDecryption=True)
        except ClientError as e:
            error = e.response.get("Error", {})
            if error.get("Code") == "ParameterNotFound":
                if silent_if_not_found:
                    return None
                raise NotFound(f"Could not find secret '{path}' in parameter store")
            raise e
        return result["Parameter"].get("Value", "")

    def list_secrets(self, path: str) -> list[str]:
        """
        List parameters at the given path.

        Returns a list of parameter names at the specified path (non-recursive).
        """
        sanitized_path = self._sanitize_path(path)
        response = self.boto3_client.get_parameters_by_path(Path=sanitized_path, Recursive=False)
        return [parameter["Name"] for parameter in response["Parameters"] if "Name" in parameter]

    def update(self, path: str, value: str) -> bool:  # type: ignore[override]
        """
        Update or create a secret as a SecureString.

        Creates the parameter if it doesn't exist, or updates it if it does.
        The value is stored as an encrypted SecureString using the default KMS key.
        """
        sanitized_path = self._sanitize_path(path)
        self.boto3_client.put_parameter(
            Name=sanitized_path,
            Value=value,
            Type="SecureString",
            Overwrite=True,
        )
        return True

    def upsert(self, path: str, value: str) -> bool:  # type: ignore[override]
        """
        Create or update a secret.

        This is an alias for update() since Parameter Store uses upsert semantics.
        """
        return self.update(path, value)

    def delete(self, path: str) -> bool:
        """
        Delete a parameter from Parameter Store.

        Returns True if the parameter was deleted, False if it didn't exist.
        """
        sanitized_path = self._sanitize_path(path)
        try:
            self.boto3_client.delete_parameter(Name=sanitized_path)
            return True
        except ClientError as e:
            error = e.response.get("Error", {})
            if error.get("Code") == "ParameterNotFound":
                return False
            raise e

    def delete_many(self, paths: list[str]) -> bool:
        """
        Delete multiple parameters from Parameter Store.

        Deletes up to 10 parameters at a time (SSM limit). For larger lists,
        recursively calls itself to delete in batches.
        """
        if not paths:
            return True
        sanitized_paths = [self._sanitize_path(p) for p in paths]
        self.boto3_client.delete_parameters(Names=sanitized_paths[:10])
        if len(sanitized_paths) > 10:
            return self.delete_many(paths[10:])
        return True

    def list_by_path(self, path: str, recursive: bool = True) -> list[str]:
        """
        List all parameter names under a given path.

        Returns a list of parameter names (not values) under the specified path.
        Uses pagination to handle large result sets.
        """
        sanitized_path = self._sanitize_path(path)
        names: list[str] = []
        paginator = self.boto3_client.get_paginator("get_parameters_by_path")
        for page in paginator.paginate(Path=sanitized_path, Recursive=recursive):
            names.extend([param["Name"] for param in page.get("Parameters", [])])
        return names

    def list_sub_folders(
        self,
        path: str,
    ) -> list[Any]:
        """
        List sub-folders at the given path.

        This operation is not supported by Parameter Store.
        """
        raise NotImplementedError("Parameter store doesn't support list_sub_folders.")
