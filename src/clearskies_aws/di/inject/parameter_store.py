from clearskies.di.injectable import Injectable

from clearskies_aws.secrets.parameter_store import (
    ParameterStore as ParameterStoreDependency,
)


class ParameterStore(Injectable):
    """
    Inject the Parameter Store secrets helper through clearskies DI.

    This inject helper returns the configured `parameter_store` dependency used
    to fetch values from AWS Systems Manager Parameter Store.

    ### Usage

    ```python
    from clearskies.di.inject import Di
    from clearskies_aws.di import inject


    class MyService:
        di = Di()
        parameter_store = inject.ParameterStore()

        def database_password(self):
            return self.parameter_store.get("/myapp/database/password")
    ```
    """

    def __init__(self, cache: bool = True):
        """Initialize the inject helper.

        Args:
            cache: When `True`, reuse the DI-cached dependency instance.
        """
        self.cache = cache

    def __get__(self, instance, parent) -> ParameterStoreDependency:
        if instance is None:
            return self  # type: ignore
        return self._di.build_from_name("parameter_store", cache=self.cache)
