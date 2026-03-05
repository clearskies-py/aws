from __future__ import annotations

import profile
from typing import Generic, Protocol, TypeVar

from clearskies.di.inject import Di, Environment
from clearskies.secrets import Secrets as BaseSecrets

from clearskies_aws.di import inject


class Boto3Client(Protocol):
    """Protocol for boto3 clients to enable type-safe generic return types."""

    ...


ClientT = TypeVar("ClientT", bound=Boto3Client)


class Secrets(BaseSecrets, Generic[ClientT]):
    boto3 = inject.Boto3()
    environment = Environment()

    def __init__(self):
        super().__init__()
        if not self.environment.get("AWS_REGION", True):
            raise ValueError("To use secrets manager you must use set the 'AWS_REGION' environment variable")

    @property
    def boto3_client(self) -> ClientT:
        """Return the boto3 client for the secrets manager implementation."""
        raise NotImplementedError("You must implement the client property in subclasses")
