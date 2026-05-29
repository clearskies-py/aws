from clearskies.configs import Config

from clearskies_aws.actions.assume_role import AssumeRole as AssumeRoleAction
from clearskies_aws.constants import regions


class Region(Config):
    def __set__(self, instance, value: str):
        if value is None:
            return

        if not isinstance(value, str):
            error_prefix = self._error_prefix(instance)
            raise TypeError(
                f"{error_prefix} attempt to set a value of type '{value.__class__.__name__}' to a parameter that requires a string."
            )

        if value and value not in regions:
            error_prefix = self._error_prefix(instance)
            raise ValueError(
                f"{error_prefix} attempt to set a value of '{value.__class__.__name__}' when an AWS regoin is reuired.  Supported regions: '"
                + "', '".join(regions)
            )

        instance._set_config(self, value)

    def __get__(self, instance, parent) -> str:
        if not instance:
            return self  # type: ignore
        return instance._get_config(self)
