"""SQS retry helper for exponential backoff."""

from __future__ import annotations

from typing import Any

from clearskies import Configurable, Loggable
from clearskies.configs import Callable as CallableConfig
from clearskies.configs import Integer, Select, String
from clearskies.decorators import parameters_to_properties
from clearskies.di.injectable_properties import InjectableProperties

from clearskies_aws.di import inject as aws_inject
from clearskies_aws.exceptions import MaxRetriesExceeded


class SqsRetry(Configurable, InjectableProperties, Loggable):
    """
    Helper for SQS message retry with exponential backoff.

    Available as injectable in SQS Lambda contexts via clearskies_aws.di.inject.SqsRetry.
    Uses visibility timeout extension (free) instead of re-queuing (costs $$).

    Provides automatic exponential backoff with configurable strategies. The helper can be
    injected into any handler and provides methods for scheduling retries and checking retry
    eligibility.

    Example:
        Basic usage in a handler

        ```python
        from clearskies_aws.di import inject


        class MyHandler:
            sqs_retry = inject.SqsRetry()

            def __call__(self, request_data):
                if not is_ready():
                    self.sqs_retry.retry_later("Not ready")
                    return
                # process...
        ```

    Example:
        Using custom backoff strategy

        ```python
        def custom_backoff(receive_count, base_delay, max_delay):
            return min(base_delay * receive_count * 2, max_delay)


        lambda_sqs = LambdaSqsStandard(
            endpoint, retry_config={"strategy": "custom", "backoff_callable": custom_backoff}
        )
        ```

    Example:
        Checking if retry is possible

        ```python
        from clearskies_aws.di import inject


        class SmartHandler:
            sqs_retry = inject.SqsRetry()

            def __call__(self, request_data):
                if not check_resource():
                    if self.sqs_retry.should_retry():
                        self.sqs_retry.retry_later("Resource not ready")
                    else:
                        # Handle max retries case
                        send_alert("Max retries reached for " + request_data["id"])
                    return
                # process...
        ```
    """

    sqs_client = aws_inject.SqsClient()

    # Injected from context (runtime values) - must be passed as constructor args
    queue_url = String()
    receipt_handle = String()
    receive_count = Integer(default=0)  # Starts at 1 for the first receive

    # Configuration (can be bound via DI)
    strategy = Select(
        allowed_values=["exponential", "linear", "fibonacci", "custom"], default="exponential"
    )  # Options: exponential, linear, fibonacci, custom
    base_delay = Integer(default=10)
    max_delay = Integer(default=900)  # 15 minutes
    max_retries = Integer(default=5)

    backoff_callable = CallableConfig(required=False)  # Custom backoff function

    @parameters_to_properties
    def __init__(
        self,
        queue_url: str | None = None,
        receipt_handle: str | None = None,
        receive_count: int | None = None,
        strategy: str | None = None,
        base_delay: int | None = None,
        max_delay: int | None = None,
        max_retries: int | None = None,
        backoff_callable: Any | None = None,
    ):
        """
        Initialize SqsRetry helper.

        Args:
            queue_url: URL of the SQS queue
            receipt_handle: Receipt handle for the message
            receive_count: Current receive count of the message
            **kwargs: Additional configuration options (strategy, base_delay, etc.)
        """
        self.finalize_and_validate_configuration()

    def retry_later(self, reason: str = "", delay: int | None = None) -> None:
        """
        Schedule message retry with exponential backoff.

        Extends the message visibility timeout to delay reprocessing. Uses configured
        backoff strategy unless custom delay is provided.

        Args:
            reason: Optional reason for retry (for logging)
            delay: Optional custom delay override (seconds)

        Raises:
            MaxRetriesExceeded: When receive_count >= max_retries
        """
        if self.receive_count >= self.max_retries:
            raise MaxRetriesExceeded(f"Max retries ({self.max_retries}) exceeded")

        if delay is None:
            delay = self._calculate_backoff()

        # Extend visibility timeout
        self.sqs_client.change_message_visibility(
            QueueUrl=self.queue_url,
            ReceiptHandle=self.receipt_handle,
            VisibilityTimeout=min(delay, 43200),  # Max 12 hours
        )

    def should_retry(self) -> bool:
        """Check if message can be retried based on current receive count."""
        return self.receive_count < self.max_retries

    def _calculate_backoff(self) -> int:
        """
        Calculate backoff delay based on strategy.

        Returns:
            Delay in seconds (clamped to [base_delay, max_delay])
        """
        # Use custom callable if provided and strategy is "custom"
        if self.strategy == "custom" and self.backoff_callable:
            return self._di.call_function(
                self.backoff_callable,
                receive_count=self.receive_count,
                base_delay=self.base_delay,
                max_delay=self.max_delay,
            )

        # Built-in strategies
        if self.strategy == "exponential":
            return min(self.base_delay * (2 ** (self.receive_count - 1)), self.max_delay)
        elif self.strategy == "linear":
            return min(self.base_delay * self.receive_count, self.max_delay)
        elif self.strategy == "fibonacci":
            fib = self._fibonacci(self.receive_count)
            return min(self.base_delay * fib, self.max_delay)
        return self.base_delay

    @staticmethod
    def _fibonacci(n: int) -> int:
        """
        Calculate the nth Fibonacci number.

        Args:
            n: Position in Fibonacci sequence (1-indexed)

        Returns:
            Fibonacci number at position n (sequence: 1, 1, 2, 3, 5, 8, 13, 21...)
        """
        if n <= 0:
            return 1  # Edge case: at least 1
        if n == 1:
            return 1
        if n == 2:
            return 1
        a, b = 1, 1
        for _ in range(2, n):
            a, b = b, a + b
        return b
