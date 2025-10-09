from __future__ import annotations

from clearskies.di.inject import Di, Environment
from clearskies.secrets import Secrets as BaseSecrets

from clearskies_aws.di import inject


class Secrets(BaseSecrets):
    boto3 = inject.Boto3()
    environment = Environment()

    def __init__(self):
        super().__init__()
        if not self.environment.get("AWS_REGION", True):
            raise ValueError("To use secrets manager you must use set the 'AWS_REGION' environment variable")
