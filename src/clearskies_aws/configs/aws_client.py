from clearskies.configs import Config

from clearskies_aws.clients import BaseAwsClient


class AwsClient(Config):
    def __set__(self, instance, value: BaseAwsClient):
        if not isinstance(value, BaseAwsClient):
            error_prefix = self._error_prefix(instance)
            raise TypeError(
                f"{error_prefix} attempt to set a value of type '{value.__class__.__name__}' to a parameter that requires a AwsClient."
            )
        instance._set_config(self, value)

    def __get__(self, instance, parent) -> BaseAwsClient:
        if not instance:
            return self  # type: ignore
        return instance._get_config(self)
