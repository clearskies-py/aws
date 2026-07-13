from __future__ import annotations

from types import ModuleType

from clearskies.di.injectable import Injectable


class Boto3(Injectable):
    """
    Inject the `boto3` module through clearskies DI.

    This inject helper lazily builds and returns `boto3` using the DI container,
    with optional caching.

    ### Usage

    ```python
    from clearskies.di.inject import Di
    from clearskies_aws.di import inject


    class MyService:
        di = Di()
        boto3 = inject.Boto3()

        def get_secrets_manager(self):
            return self.boto3.client("secretsmanager")
    ```
    """

    def __init__(self, cache: bool = True):
        """Initialize the inject helper.

        Args:
            cache: When `True`, reuse the DI-cached `boto3` module instance.
        """
        self.cache = cache

    def __get__(self, instance, parent) -> ModuleType:
        if instance is None:
            return self  # type: ignore
        return self._di.build_standard_lib("boto3", cache=self.cache)
