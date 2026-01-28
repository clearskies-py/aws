from __future__ import annotations

from typing import Any

from botocore.exceptions import ClientError
from clearskies.exceptions.not_found import NotFound
from types_boto3_secretsmanager import SecretsManagerClient
from types_boto3_secretsmanager.type_defs import SecretListEntryTypeDef

from clearskies_aws.secrets import secrets


class SecretsManager(secrets.Secrets[SecretsManagerClient]):
    """
    Backend for managing secrets using AWS Secrets Manager.

    This class provides integration with AWS Secrets Manager, allowing you to store,
    retrieve, update, and delete secrets. It supports versioning and KMS encryption.

    ### Example Usage

    ```python
    from clearskies_aws.secrets import SecretsManager

    secrets = SecretsManager()

    # Create a new secret
    secrets.create("my-app/database-password", "super-secret-password")

    # Get a secret
    password = secrets.get("my-app/database-password")

    # Update a secret
    secrets.update("my-app/database-password", "new-password")

    # Delete a secret
    secrets.delete("my-app/database-password")
    ```
    """

    secrets_manager: SecretsManagerClient

    def __init__(self):
        """Initialize the Secrets Manager backend."""
        super().__init__()

    @property
    def boto3_client(self) -> SecretsManagerClient:
        """
        Return the boto3 Secrets Manager client.

        Creates a new client if one doesn't exist yet, using the AWS_REGION environment variable.
        """
        if hasattr(self, "secrets_manager"):
            return self.secrets_manager
        self.secrets_manager = self.boto3.client(
            "secretsmanager",
            region_name=self.environment.get("AWS_REGION"),
        )
        return self.secrets_manager

    def create(self, secret_id: str, value: Any, kms_key_id: str | None = None) -> bool:
        """
        Create a new secret in Secrets Manager.

        Creates a new secret with the given ID and value. Optionally encrypts the secret
        with a custom KMS key. Returns True if the secret was created successfully.
        """
        calling_parameters = {
            "SecretId": secret_id,
            "SecretString": value,
            "KmsKeyId": kms_key_id,
        }
        calling_parameters = {key: value for (key, value) in calling_parameters.items() if value}
        result = self.boto3_client.create_secret(**calling_parameters)
        return bool(result.get("ARN"))

    def get(  # type: ignore[override]
        self,
        secret_id: str,
        version_id: str | None = None,
        version_stage: str | None = None,
        silent_if_not_found: bool = False,
    ) -> str | bytes | None:
        """
        Retrieve a secret value from Secrets Manager.

        Returns the secret value for the given ID. Optionally retrieves a specific version
        by version_id or version_stage. If silent_if_not_found is True, returns None when
        the secret is not found instead of raising NotFound.
        """
        calling_parameters = {"SecretId": secret_id}

        # Only add optional parameters if they are not None
        if version_id:
            calling_parameters["VersionId"] = version_id
        if version_stage:
            calling_parameters["VersionStage"] = version_stage

        try:
            result = self.boto3_client.get_secret_value(**calling_parameters)
        except ClientError as e:
            error = e.response.get("Error", {})
            if error.get("Code") == "ResourceNotFoundException":
                if silent_if_not_found:
                    return None
                raise NotFound(
                    f"Could not find secret '{secret_id}' with version '{version_id}' and stage '{version_stage}'"
                )
            raise e
        if result.get("SecretString"):
            return result.get("SecretString")
        return result.get("SecretBinary")

    def list_secrets(self, path: str) -> list[SecretListEntryTypeDef]:  # type: ignore[override]
        """
        List secrets matching the given path filter.

        Returns a list of secret metadata entries that match the path filter.
        """
        results = self.boto3_client.list_secrets(
            Filters=[
                {
                    "Key": "name",
                    "Values": [path],
                },
            ],
        )
        return results["SecretList"]

    def update(self, secret_id: str, value: str, kms_key_id: str | None = None) -> bool:  # type: ignore[override]
        """
        Update an existing secret's value.

        Updates the secret with the given ID to the new value. Optionally re-encrypts
        with a different KMS key. Returns True if the update was successful.
        """
        calling_parameters = {
            "SecretId": secret_id,
            "SecretString": value,
        }
        if kms_key_id:
            # If no KMS key is provided, we should not include it in the parameters
            calling_parameters["KmsKeyId"] = kms_key_id

        result = self.boto3_client.update_secret(**calling_parameters)
        return bool(result.get("ARN"))

    def upsert(self, secret_id: str, value: str, kms_key_id: str | None = None) -> bool:  # type: ignore[override]
        """
        Create or update a secret value.

        Creates a new version of the secret with the given value. This is useful for
        rotating secrets. Returns True if the operation was successful.
        """
        calling_parameters = {
            "SecretId": secret_id,
            "SecretString": value,
        }
        if kms_key_id:
            # If no KMS key is provided, we should not include it in the parameters
            calling_parameters["KmsKeyId"] = kms_key_id

        result = self.boto3_client.put_secret_value(**calling_parameters)
        return bool(result.get("ARN"))

    def delete(self, secret_id: str, force_delete: bool = False) -> bool:
        """
        Delete a secret from Secrets Manager.

        If force_delete is True, the secret is deleted immediately without recovery window.
        Otherwise, the secret is scheduled for deletion with a 7-day recovery window.
        Returns True if the secret was deleted, False if it didn't exist.
        """
        try:
            if force_delete:
                self.boto3_client.delete_secret(SecretId=secret_id, ForceDeleteWithoutRecovery=True)
            else:
                self.boto3_client.delete_secret(SecretId=secret_id)
            return True
        except ClientError as e:
            error = e.response.get("Error", {})
            if error.get("Code") == "ResourceNotFoundException":
                return False
            raise e

    def list_sub_folders(self, path: str, value: str) -> list[str]:  # type: ignore[override]
        """
        List sub-folders at the given path.

        This operation is not supported by Secrets Manager.
        """
        raise NotImplementedError("Secrets Manager doesn't support list_sub_folders.")
