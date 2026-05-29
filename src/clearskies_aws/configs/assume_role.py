from clearskies.configs import Config

from clearskies_aws.actions.assume_role import AssumeRole as AssumeRoleAction


class AssumeRole(Config):
    def __set__(self, instance, value: AssumeRoleAction | list[AssumeRoleAction]):
        if not isinstance(value, list) and not isinstance(value, AssumeRoleAction):
            error_prefix = self._error_prefix(instance)
            raise TypeError(
                f"{error_prefix} attempt to set a value of type '{value.__class__.__name__}' to a parameter that requires an instance of clearskies_aws.actions.AssumeRole or a list of such instances."
            )

        if isinstance(value, list):
            for index, item in enumerate(value):
                if not isinstance(item, AssumeRoleAction):
                    error_prefix = self._error_prefix(instance)
                    raise TypeError(
                        f"{error_prefix} attempt to set a value of type '{item.__class__.__name__}' for item #{index + 1}. An instance of clearskies_aws.actions.AssumeRole was expected."
                    )
        else:
            value = [value]

        instance._set_config(self, [*value])

    def __get__(self, instance, parent) -> list[AssumeRoleAction]:
        if not instance:
            return self  # type: ignore
        return instance._get_config(self)
