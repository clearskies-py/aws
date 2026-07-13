from types import ModuleType

from clearskies.di.injectable import Injectable


class Boto3Session(Injectable):
    """
    Inject the shared boto3 session through clearskies DI.

    This inject helper returns the configured `boto3_session` dependency from the
    DI container.

    ### Usage

    ```python
    from clearskies.di.inject import Di
    from clearskies_aws.di import inject


    class MyService:
        di = Di()
        boto3_session = inject.Boto3Session()

        def sts_client(self):
            return self.boto3_session.client("sts")
    ```
    """

    def __init__(self, cache: bool = True):
        """Initialize the inject helper.

        Args:
            cache: When `True`, reuse the DI-cached session instance.
        """
        self.cache = cache

    def __get__(self, instance, parent) -> ModuleType:
        if instance is None:
            return self  # type: ignore
        return self._di.build_from_name("boto3_session", cache=self.cache)
