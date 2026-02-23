"""SQS-specific exceptions for retry handling."""

from __future__ import annotations


class SqsRetryException(Exception):
    """
    Base exception for SQS retries.

    When raised in an SQS Lambda handler, the context will automatically
    retry the message with exponential backoff using visibility timeout extension.
    This avoids the cost and metadata loss of manual re-queuing.

    Example:
        Raising with default backoff

        ```python
        from clearskies_aws.exceptions import SqsRetryException


        def my_handler(request_data):
            if not resource_ready():
                raise SqsRetryException("Resource not available")
        ```

    Example:
        Raising with custom delay

        ```python
        from clearskies_aws.exceptions import SqsRetryException


        def my_handler(request_data):
            if api_rate_limited():
                raise SqsRetryException("Rate limited", delay=120)
        ```
    """

    def __init__(self, message: str, delay: int | None = None):
        """
        Initialize retry exception.

        Args:
            message: Error message describing why retry is needed
            delay: Optional custom delay override in seconds
        """
        self.delay = delay
        super().__init__(message)


class SqsNotReadyException(SqsRetryException):
    """
    Resource not yet available exception.

    Raised when checking for resources that may become available later,
    such as database records that haven't been created yet or files that
    are still being uploaded.

    Example:
        Waiting for database record

        ```python
        from clearskies_aws.exceptions import SqsNotReadyException


        def process_order(request_data, models):
            order = models.orders.find(request_data["order_id"])
            if not order:
                raise SqsNotReadyException("Order not found yet")
            # Process order...
        ```

    Example:
        Waiting for S3 file

        ```python
        from clearskies_aws.exceptions import SqsNotReadyException
        import boto3


        def process_file(request_data):
            s3 = boto3.client("s3")
            try:
                s3.head_object(Bucket="my-bucket", Key=request_data["file_key"])
            except s3.exceptions.NoSuchKey:
                raise SqsNotReadyException(f"File {request_data['file_key']} not yet uploaded")
            # Process file...
        ```
    """

    pass


class SqsTransientErrorException(SqsRetryException):
    """
    Transient error exception.

    Raised for temporary failures like network timeouts, rate limits,
    or service unavailability that may succeed on retry.

    Example:
        API timeout

        ```python
        from clearskies_aws.exceptions import SqsTransientErrorException
        import requests


        def call_external_api(request_data):
            try:
                response = requests.post(API_URL, json=request_data, timeout=10)
                response.raise_for_status()
            except requests.Timeout:
                raise SqsTransientErrorException("API timeout")
            except requests.ConnectionError:
                raise SqsTransientErrorException("API connection error")
        ```

    Example:
        Rate limiting

        ```python
        from clearskies_aws.exceptions import SqsTransientErrorException
        import requests


        def call_rate_limited_api(request_data):
            response = requests.get(API_URL)
            if response.status_code == 429:
                # Custom delay for rate limits
                raise SqsTransientErrorException("Rate limited", delay=300)
            # Process response...
        ```
    """

    pass


class SqsPermanentErrorException(Exception):
    """
    Permanent error exception.

    Raised when a message should not be retried due to validation failures,
    malformed data, or other permanent errors. The message will be sent
    directly to the Dead Letter Queue (DLQ) if configured.

    Example:
        Validation error

        ```python
        from clearskies_aws.exceptions import SqsPermanentErrorException


        def validate_and_process(request_data):
            if not request_data.get("required_field"):
                raise SqsPermanentErrorException("Missing required_field")
            if not isinstance(request_data.get("amount"), (int, float)):
                raise SqsPermanentErrorException("Invalid amount type")
            # Process data...
        ```

    Example:
        Business logic error

        ```python
        from clearskies_aws.exceptions import SqsPermanentErrorException


        def process_payment(request_data, models):
            order = models.orders.find(request_data["order_id"])
            if order.status == "cancelled":
                raise SqsPermanentErrorException("Cannot process payment for cancelled order")
            # Process payment...
        ```
    """

    pass


class MaxRetriesExceeded(Exception):
    """
    Maximum retries exceeded exception.

    Raised when the message receive count has reached the configured maximum
    retry limit. The message will be sent to the Dead Letter Queue (DLQ).

    This exception is typically raised internally by the retry helper or context,
    not by user code.
    """

    pass
