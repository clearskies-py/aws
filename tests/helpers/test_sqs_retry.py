"""Tests for SqsRetry helper class."""

from __future__ import annotations

import unittest
from unittest.mock import MagicMock, Mock

from clearskies.di import Di

from clearskies_aws.exceptions import MaxRetriesExceeded
from clearskies_aws.helpers.sqs_retry import SqsRetry


class SqsRetryTest(unittest.TestCase):
    """Test SqsRetry helper functionality."""

    def setUp(self):
        """Set up test dependencies."""
        self.mock_boto3_client = Mock()
        # SqsClient wrapper should be callable returning the boto3 client
        self.mock_sqs_client_wrapper = Mock(return_value=self.mock_boto3_client)
        self.di = Di(bindings={"sqs_client": self.mock_sqs_client_wrapper})

    def test_exponential_backoff_calculation(self):
        """Test exponential backoff calculation."""
        helper = SqsRetry(
            queue_url="https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            receipt_handle="test-receipt-handle",
            receive_count=1,
            strategy="exponential",
            base_delay=10,
            max_delay=900,
            max_retries=5,
        )
        helper._di = self.di

        SqsRetry.sqs_client._di = self.di

        # receive_count=1: 10 * 2^0 = 10
        self.assertEqual(helper._calculate_backoff(), 10)

        # receive_count=2: 10 * 2^1 = 20
        helper.receive_count = 2
        self.assertEqual(helper._calculate_backoff(), 20)

        # receive_count=3: 10 * 2^2 = 40
        helper.receive_count = 3
        self.assertEqual(helper._calculate_backoff(), 40)

        # receive_count=10: should hit max_delay
        helper.receive_count = 10
        self.assertEqual(helper._calculate_backoff(), 900)

    def test_linear_backoff_calculation(self):
        """Test linear backoff calculation."""
        helper = SqsRetry(
            queue_url="https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            receipt_handle="test-receipt-handle",
            receive_count=1,
            strategy="linear",
            base_delay=10,
            max_delay=900,
            max_retries=5,
        )
        helper._di = self.di

        SqsRetry.sqs_client._di = self.di

        # receive_count=1: 10 * 1 = 10
        self.assertEqual(helper._calculate_backoff(), 10)

        # receive_count=2: 10 * 2 = 20
        helper.receive_count = 2
        self.assertEqual(helper._calculate_backoff(), 20)

        # receive_count=5: 10 * 5 = 50
        helper.receive_count = 5
        self.assertEqual(helper._calculate_backoff(), 50)

        # receive_count=100: should hit max_delay
        helper.receive_count = 100
        self.assertEqual(helper._calculate_backoff(), 900)

    def test_fibonacci_backoff_calculation(self):
        """Test fibonacci backoff calculation."""
        helper = SqsRetry(
            queue_url="https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            receipt_handle="test-receipt-handle",
            receive_count=1,
            strategy="fibonacci",
            base_delay=10,
            max_delay=900,
            max_retries=5,
        )
        helper._di = self.di

        SqsRetry.sqs_client._di = self.di

        # Fibonacci sequence: 1, 1, 2, 3, 5, 8, 13, 21...
        # receive_count=1: 10 * 1 = 10
        self.assertEqual(helper._calculate_backoff(), 10)

        # receive_count=2: 10 * 1 = 10
        helper.receive_count = 2
        self.assertEqual(helper._calculate_backoff(), 10)

        # receive_count=3: 10 * 2 = 20
        helper.receive_count = 3
        self.assertEqual(helper._calculate_backoff(), 20)

        # receive_count=4: 10 * 3 = 30
        helper.receive_count = 4
        self.assertEqual(helper._calculate_backoff(), 30)

        # receive_count=5: 10 * 5 = 50
        helper.receive_count = 5
        self.assertEqual(helper._calculate_backoff(), 50)

    def test_custom_backoff_callable(self):
        """Test custom backoff callable."""

        def custom_backoff(receive_count, base_delay, max_delay):
            # Custom: multiply receive count by 30 seconds
            return min(receive_count * 30, max_delay)

        helper = SqsRetry(
            queue_url="https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            receipt_handle="test-receipt-handle",
            receive_count=2,
            strategy="custom",
            base_delay=10,
            max_delay=900,
            max_retries=5,
            backoff_callable=custom_backoff,
        )
        helper._di = self.di

        SqsRetry.sqs_client._di = self.di

        # receive_count=2: 2 * 30 = 60
        self.assertEqual(helper._calculate_backoff(), 60)

    def test_should_retry_true(self):
        """Test should_retry returns True when under max_retries."""
        helper = SqsRetry(
            queue_url="https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            receipt_handle="test-receipt-handle",
            receive_count=3,
            max_retries=5,
        )
        helper._di = self.di

        self.assertTrue(helper.should_retry())

    def test_should_retry_false(self):
        """Test should_retry returns False when at max_retries."""
        helper = SqsRetry(
            queue_url="https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            receipt_handle="test-receipt-handle",
            receive_count=5,
            max_retries=5,
        )
        helper._di = self.di

        self.assertFalse(helper.should_retry())

    def test_should_retry_false_exceeded(self):
        """Test should_retry returns False when exceeded max_retries."""
        helper = SqsRetry(
            queue_url="https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            receipt_handle="test-receipt-handle",
            receive_count=6,
            max_retries=5,
        )
        helper._di = self.di

        self.assertFalse(helper.should_retry())

    def test_retry_later_calls_sqs_client(self):
        """Test retry_later calls SQS change_message_visibility."""
        helper = SqsRetry(
            queue_url="https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            receipt_handle="test-receipt-handle",
            receive_count=2,
            strategy="exponential",
            base_delay=10,
            max_retries=5,
        )
        helper._di = self.di

        SqsRetry.sqs_client._di = self.di

        helper.retry_later("Resource not ready")

        # Should call change_message_visibility with calculated delay (20 seconds for receive_count=2)
        self.mock_boto3_client.change_message_visibility.assert_called_once_with(
            QueueUrl="https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            ReceiptHandle="test-receipt-handle",
            VisibilityTimeout=20,
        )

    def test_retry_later_with_custom_delay(self):
        """Test retry_later with custom delay override."""
        helper = SqsRetry(
            queue_url="https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            receipt_handle="test-receipt-handle",
            receive_count=2,
            max_retries=5,
        )
        helper._di = self.di

        SqsRetry.sqs_client._di = self.di

        helper.retry_later("Resource not ready", delay=300)

        # Should use custom delay instead of calculated
        self.mock_boto3_client.change_message_visibility.assert_called_once_with(
            QueueUrl="https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            ReceiptHandle="test-receipt-handle",
            VisibilityTimeout=300,
        )

    def test_retry_later_enforces_max_visibility_timeout(self):
        """Test retry_later enforces AWS max visibility timeout of 12 hours (43200 seconds)."""
        helper = SqsRetry(
            queue_url="https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            receipt_handle="test-receipt-handle",
            receive_count=2,
            max_retries=5,
        )
        helper._di = self.di

        SqsRetry.sqs_client._di = self.di

        # Try to set huge delay
        helper.retry_later("Resource not ready", delay=100000)

        # Should cap at 12 hours
        self.mock_boto3_client.change_message_visibility.assert_called_once_with(
            QueueUrl="https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            ReceiptHandle="test-receipt-handle",
            VisibilityTimeout=43200,
        )

    def test_retry_later_raises_max_retries_exceeded(self):
        """Test retry_later raises MaxRetriesExceeded when at limit."""
        helper = SqsRetry(
            queue_url="https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            receipt_handle="test-receipt-handle",
            receive_count=5,
            max_retries=5,
        )
        helper._di = self.di

        SqsRetry.sqs_client._di = self.di

        with self.assertRaises(MaxRetriesExceeded) as context:
            helper.retry_later("Resource not ready")

        self.assertIn("Max retries (5) exceeded", str(context.exception))

        # Should NOT call SQS client
        self.mock_boto3_client.change_message_visibility.assert_not_called()

    def test_fibonacci_sequence_correctness(self):
        """Test fibonacci sequence generation is correct."""
        helper = SqsRetry()

        # Test known fibonacci numbers
        self.assertEqual(helper._fibonacci(0), 1)  # Edge case: at least 1
        self.assertEqual(helper._fibonacci(1), 1)
        self.assertEqual(helper._fibonacci(2), 1)
        self.assertEqual(helper._fibonacci(3), 2)
        self.assertEqual(helper._fibonacci(4), 3)
        self.assertEqual(helper._fibonacci(5), 5)
        self.assertEqual(helper._fibonacci(6), 8)
        self.assertEqual(helper._fibonacci(7), 13)
        self.assertEqual(helper._fibonacci(8), 21)

    def test_fibonacci_backoff(self):
        """Test backoff calculation with different base delays."""
        helper = SqsRetry(
            queue_url="https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            receipt_handle="test-receipt-handle",
            receive_count=3,
            strategy="exponential",
            base_delay=30,
            max_delay=900,
            max_retries=5,
        )

        # Test known fibonacci numbers
        self.assertEqual(helper._fibonacci(0), 1)  # Edge case: at least 1
        self.assertEqual(helper._fibonacci(1), 1)
        self.assertEqual(helper._fibonacci(2), 1)
        self.assertEqual(helper._fibonacci(3), 2)
        self.assertEqual(helper._fibonacci(4), 3)
        self.assertEqual(helper._fibonacci(5), 5)
        self.assertEqual(helper._fibonacci(6), 8)
        self.assertEqual(helper._fibonacci(7), 13)
        self.assertEqual(helper._fibonacci(8), 21)

    def test_different_base_delays(self):
        """Test backoff calculation with different base delays."""
        helper = SqsRetry(
            queue_url="https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            receipt_handle="test-receipt-handle",
            receive_count=3,
            strategy="exponential",
            base_delay=30,
            max_delay=900,
            max_retries=5,
        )
        helper._di = self.di

        SqsRetry.sqs_client._di = self.di

        # receive_count=3: 30 * 2^2 = 120
        self.assertEqual(helper._calculate_backoff(), 120)
