"""
AWS Parameter Store implementation of SecretCache.

This module provides a cache storage backend using AWS Systems Manager Parameter Store.
"""

from __future__ import annotations

from clearskies.configs import Boolean, String
from clearskies.decorators import parameters_to_properties
from clearskies.di.inject import ByClass
from clearskies.secrets.cache_storage import SecretCache

from clearskies_aws.di import inject
from clearskies_aws.secrets import parameter_store


class ParameterStoreCache(SecretCache):
    """
    Cache storage backend using AWS Systems Manager Parameter Store.

    This class implements the SecretCache interface to store cached secrets in AWS
    Parameter Store. Paths are automatically sanitized by the underlying ParameterStore
    to comply with SSM parameter naming requirements.

    ### Example Usage

    ```python
    from clearskies.secrets import Akeyless
    from clearskies_aws.secrets.secrets_cache import ParameterStoreCache

    cache = ParameterStoreCache(prefix="/cache/secrets")
    akeyless = Akeyless(
        access_id="p-xxx",
        access_type="aws_iam",
        cache_storage=cache,
    )

    # First call fetches from Akeyless and caches in Parameter Store
    secret = akeyless.get("/path/to/secret")

    # Subsequent calls return from Parameter Store cache
    secret = akeyless.get("/path/to/secret")

    # Force refresh bypasses cache
    secret = akeyless.get("/path/to/secret", refresh=True)
    ```
    """

    boto3 = inject.Boto3()

    parameter_store = ByClass(parameter_store.ParameterStore)

    prefix = String(default=None)
    allow_cleanup = Boolean(default=False)

    @parameters_to_properties
    def __init__(self, prefix: str | None = None, allow_cleanup: bool = False) -> None:
        """
        Initialize the Parameter Store cache.

        The prefix is prepended to all secret paths when storing in Parameter Store.
        This helps organize cached secrets and avoid conflicts with other parameters.
        """
        super().__init__()

    def _build_path(self, path: str) -> str:
        """
        Build the full parameter path by prepending the prefix.

        The path is sanitized by the underlying ParameterStore.
        """
        return f"{self.prefix}/{path.lstrip('/')}"

    def get(self, path: str) -> str | None:
        """
        Retrieve a cached secret value from Parameter Store.

        Returns the cached secret value for the given path, or None if not found.
        """
        ssm_name = self._build_path(path)
        return self.parameter_store.get(ssm_name, silent_if_not_found=True)

    def set(self, path: str, value: str, ttl: int | None = None) -> None:
        """
        Store a secret value in Parameter Store.

        Stores the secret value as a SecureString parameter. Note that Parameter Store
        does not natively support TTL, so the ttl parameter is ignored. Consider using
        a cleanup process or Lambda function to remove stale cached secrets.
        """
        ssm_name = self._build_path(path)
        self.parameter_store.update(ssm_name, value)

    def delete(self, path: str) -> None:
        """
        Remove a secret from the Parameter Store cache.

        Deletes the parameter at the given path. Does nothing if the parameter
        doesn't exist.
        """
        ssm_name = self._build_path(path)
        self.parameter_store.delete(ssm_name)

    def clear(self) -> None:
        """
        Remove all cached secrets from Parameter Store under the configured prefix.

        This method deletes all parameters under the cache prefix. Use with caution
        in production environments.
        """
        if not self.allow_cleanup:
            raise RuntimeError(
                "Clearing the Parameter Store cache is not allowed. Set allow_cleanup=True to enable this operation."
            )
        names = self.parameter_store.list_by_path(self.prefix, recursive=True)
        if names:
            self.parameter_store.delete_many(names)
