"""Tests for LambdaSqsStandard context with retry handling."""

from __future__ import annotations

import json
import unittest
from unittest.mock import Mock, patch

import clearskies

from clearskies_aws.contexts.lambda_sqs_standard import LambdaSqsStandard
from clearskies_aws.exceptions import (
    MaxRetriesExceeded,
    SqsNotReadyException,
    SqsPermanentErrorException,
    SqsRetryException,
    SqsTransientErrorException,
)

from .my_awesome_model import MyAwesomeModel


def throw(request_data):
    """Test handler that raises ValueError for 'bob'."""
    if request_data["name"] == "bob":
        raise ValueError("SUP!")


def throw_sqs_not_ready(request_data):
    """Test handler that raises SqsNotReadyException."""
    if request_data["name"] == "bob":
        raise SqsNotReadyException("Resource not ready")


def throw_sqs_transient_error(request_data):
    """Test handler that raises SqsTransientErrorException."""
    if request_data["name"] == "bob":
        raise SqsTransientErrorException("API timeout")


def throw_sqs_permanent_error(request_data):
    """Test handler that raises SqsPermanentErrorException."""
    if request_data["name"] == "bob":
        raise SqsPermanentErrorException("Invalid data")


def throw_max_retries_exceeded(request_data):
    """Test handler that raises MaxRetriesExceeded."""
    if request_data["name"] == "bob":
        raise MaxRetriesExceeded("Max retries exceeded")


class LambdaSqsStandardTest(unittest.TestCase):
    """Test LambdaSqsStandard context."""

    def setUp(self):
        """Set up test dependencies."""
        clearskies.backends.MemoryBackend.clear_table_cache()

    def test_create(self):
        """Test basic create operation."""
        application = LambdaSqsStandard(
            clearskies.endpoints.Create(
                MyAwesomeModel,
                readable_column_names=["id", "name", "email", "created_at"],
                writeable_column_names=["name", "email"],
                url="/create",
            )
        )

        application(
            {
                "Records": [
                    {
                        "messageId": "1-2-3-4",
                        "receiptHandle": "receipt-1",
                        "eventSourceARN": "arn:aws:sqs:us-east-1:123456789:test-queue",
                        "body": json.dumps({"name": "Bob", "email": "bob@example.com"}),
                        "attributes": {"ApproximateReceiveCount": "1"},
                    },
                    {
                        "messageId": "2-3-4-5",
                        "receiptHandle": "receipt-2",
                        "eventSourceARN": "arn:aws:sqs:us-east-1:123456789:test-queue",
                        "body": json.dumps({"name": "Jane", "email": "jane@example.com"}),
                        "attributes": {"ApproximateReceiveCount": "1"},
                    },
                ]
            },
            {},
            url="/create",
            request_method="POST",
        )

        # there's no response from SQS, so check the data in the backend
        models = application.build(MyAwesomeModel)
        assert len(models) == 2
        assert ["bob@example.com", "jane@example.com"] == [model.email for model in models]

    def test_with_failure(self):
        """Test with standard exception failure."""
        records = {
            "Records": [
                {
                    "messageId": "1-2-3-4",
                    "receiptHandle": "receipt-1",
                    "eventSourceARN": "arn:aws:sqs:us-east-1:123456789:test-queue",
                    "body": json.dumps({"name": "jane"}),
                    "attributes": {"ApproximateReceiveCount": "1"},
                },
                {
                    "messageId": "2-3-4-5",
                    "receiptHandle": "receipt-2",
                    "eventSourceARN": "arn:aws:sqs:us-east-1:123456789:test-queue",
                    "body": json.dumps({"name": "bob"}),
                    "attributes": {"ApproximateReceiveCount": "1"},
                },
            ]
        }
        sqs_handler = LambdaSqsStandard(clearskies.endpoints.Callable(throw))
        response = sqs_handler(records, {})

        self.assertEqual(
            {"batchItemFailures": [{"itemIdentifier": "2-3-4-5"}]},
            response,
        )

    def test_sqs_not_ready_exception_extends_visibility(self):
        """Test SqsNotReadyException extends visibility timeout."""
        # Create mock SQS client that returns a callable (mimicking SqsClient)
        mock_boto3_client = Mock()
        mock_sqs_client_wrapper = Mock(return_value=mock_boto3_client)

        records = {
            "Records": [
                {
                    "messageId": "1-2-3-4",
                    "receiptHandle": "receipt-handle-123",
                    "eventSourceARN": "arn:aws:sqs:us-east-1:123456789:test-queue",
                    "body": json.dumps({"name": "bob"}),
                    "attributes": {"ApproximateReceiveCount": "2"},
                },
            ]
        }
        sqs_handler = LambdaSqsStandard(
            clearskies.endpoints.Callable(throw_sqs_not_ready), bindings={"sqs_client": mock_sqs_client_wrapper}
        )
        response = sqs_handler(records, {})

        # Should NOT have batch failures (visibility was extended)
        self.assertEqual({}, response)

        # Should have called change_message_visibility
        mock_boto3_client.change_message_visibility.assert_called_once()
        call_args = mock_boto3_client.change_message_visibility.call_args
        self.assertEqual(call_args[1]["QueueUrl"], "https://sqs.us-east-1.amazonaws.com/123456789/test-queue")
        self.assertEqual(call_args[1]["ReceiptHandle"], "receipt-handle-123")
        # receive_count=2: delay should be 20 seconds (10 * 2^1)
        self.assertEqual(call_args[1]["VisibilityTimeout"], 20)

    def test_sqs_transient_error_exception_extends_visibility(self):
        """Test SqsTransientErrorException extends visibility timeout."""
        mock_boto3_client = Mock()
        mock_sqs_client_wrapper = Mock(return_value=mock_boto3_client)

        records = {
            "Records": [
                {
                    "messageId": "1-2-3-4",
                    "receiptHandle": "receipt-handle-123",
                    "eventSourceARN": "arn:aws:sqs:us-east-1:123456789:test-queue",
                    "body": json.dumps({"name": "bob"}),
                    "attributes": {"ApproximateReceiveCount": "1"},
                },
            ]
        }
        sqs_handler = LambdaSqsStandard(
            clearskies.endpoints.Callable(throw_sqs_transient_error), bindings={"sqs_client": mock_sqs_client_wrapper}
        )
        response = sqs_handler(records, {})

        # Should NOT have batch failures
        self.assertEqual({}, response)

        # Should have called change_message_visibility
        mock_boto3_client.change_message_visibility.assert_called_once()

    def test_sqs_permanent_error_exception_sends_to_dlq(self):
        """Test SqsPermanentErrorException sends message to DLQ."""
        records = {
            "Records": [
                {
                    "messageId": "1-2-3-4",
                    "receiptHandle": "receipt-handle-123",
                    "eventSourceARN": "arn:aws:sqs:us-east-1:123456789:test-queue",
                    "body": json.dumps({"name": "bob"}),
                    "attributes": {"ApproximateReceiveCount": "1"},
                },
            ]
        }
        sqs_handler = LambdaSqsStandard(clearskies.endpoints.Callable(throw_sqs_permanent_error))
        response = sqs_handler(records, {})

        # Should have batch failure (send to DLQ)
        self.assertEqual(
            {"batchItemFailures": [{"itemIdentifier": "1-2-3-4"}]},
            response,
        )

    def test_max_retries_exceeded_sends_to_dlq(self):
        """Test MaxRetriesExceeded sends message to DLQ."""
        records = {
            "Records": [
                {
                    "messageId": "1-2-3-4",
                    "receiptHandle": "receipt-handle-123",
                    "eventSourceARN": "arn:aws:sqs:us-east-1:123456789:test-queue",
                    "body": json.dumps({"name": "bob"}),
                    "attributes": {"ApproximateReceiveCount": "6"},
                },
            ]
        }
        sqs_handler = LambdaSqsStandard(clearskies.endpoints.Callable(throw_max_retries_exceeded))
        response = sqs_handler(records, {})

        # Should have batch failure (send to DLQ)
        self.assertEqual(
            {"batchItemFailures": [{"itemIdentifier": "1-2-3-4"}]},
            response,
        )

    def test_retry_exception_at_max_retries_sends_to_dlq(self):
        """Test retry exception at max retries sends to DLQ."""
        mock_boto3_client = Mock()
        mock_sqs_client_wrapper = Mock(return_value=mock_boto3_client)

        records = {
            "Records": [
                {
                    "messageId": "1-2-3-4",
                    "receiptHandle": "receipt-handle-123",
                    "eventSourceARN": "arn:aws:sqs:us-east-1:123456789:test-queue",
                    "body": json.dumps({"name": "bob"}),
                    "attributes": {"ApproximateReceiveCount": "5"},  # At max retries
                },
            ]
        }
        sqs_handler = LambdaSqsStandard(
            clearskies.endpoints.Callable(throw_sqs_not_ready), bindings={"sqs_client": mock_sqs_client_wrapper}
        )
        response = sqs_handler(records, {})

        # Should have batch failure (max retries reached)
        self.assertEqual(
            {"batchItemFailures": [{"itemIdentifier": "1-2-3-4"}]},
            response,
        )

        # Should NOT have called change_message_visibility
        mock_boto3_client.change_message_visibility.assert_not_called()

    def test_custom_delay_in_exception(self):
        """Test custom delay specified in exception."""
        mock_boto3_client = Mock()
        mock_sqs_client_wrapper = Mock(return_value=mock_boto3_client)

        def throw_with_custom_delay(request_data):
            raise SqsRetryException("Custom delay", delay=300)

        records = {
            "Records": [
                {
                    "messageId": "1-2-3-4",
                    "receiptHandle": "receipt-handle-123",
                    "eventSourceARN": "arn:aws:sqs:us-east-1:123456789:test-queue",
                    "body": json.dumps({"name": "bob"}),
                    "attributes": {"ApproximateReceiveCount": "1"},
                },
            ]
        }
        sqs_handler = LambdaSqsStandard(
            clearskies.endpoints.Callable(throw_with_custom_delay), bindings={"sqs_client": mock_sqs_client_wrapper}
        )
        response = sqs_handler(records, {})

        # Should NOT have batch failures
        self.assertEqual({}, response)

        # Should use custom delay
        call_args = mock_boto3_client.change_message_visibility.call_args
        self.assertEqual(call_args[1]["VisibilityTimeout"], 300)

    def test_extract_queue_url_from_arn(self):
        """Test _extract_queue_url correctly parses ARN."""
        context = LambdaSqsStandard(clearskies.endpoints.Callable(lambda x: x))

        arn = "arn:aws:sqs:us-east-1:123456789012:my-queue-name"
        expected = "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue-name"

        self.assertEqual(context._extract_queue_url(arn), expected)

    def test_extract_queue_url_handles_empty_arn(self):
        """Test _extract_queue_url handles empty ARN."""
        context = LambdaSqsStandard(clearskies.endpoints.Callable(lambda x: x))

        self.assertEqual(context._extract_queue_url(""), "")

    def test_calculate_backoff(self):
        """Test _calculate_backoff exponential calculation."""
        context = LambdaSqsStandard(clearskies.endpoints.Callable(lambda x: x))

        # receive_count=1: 10 * 2^0 = 10
        self.assertEqual(context._calculate_backoff(1), 10)

        # receive_count=2: 10 * 2^1 = 20
        self.assertEqual(context._calculate_backoff(2), 20)

        # receive_count=3: 10 * 2^2 = 40
        self.assertEqual(context._calculate_backoff(3), 40)

        # Should cap at max_delay
        self.assertEqual(context._calculate_backoff(10), 900)

    def test_mixed_success_and_retry(self):
        """Test batch with mix of success and retry messages."""

        def mixed_handler(request_data):
            if request_data["name"] == "bob":
                raise SqsNotReadyException("Not ready")
            # Otherwise succeed

        mock_boto3_client = Mock()
        mock_sqs_client_wrapper = Mock(return_value=mock_boto3_client)

        records = {
            "Records": [
                {
                    "messageId": "1-2-3-4",
                    "receiptHandle": "receipt-1",
                    "eventSourceARN": "arn:aws:sqs:us-east-1:123456789:test-queue",
                    "body": json.dumps({"name": "alice"}),
                    "attributes": {"ApproximateReceiveCount": "1"},
                },
                {
                    "messageId": "2-3-4-5",
                    "receiptHandle": "receipt-2",
                    "eventSourceARN": "arn:aws:sqs:us-east-1:123456789:test-queue",
                    "body": json.dumps({"name": "bob"}),
                    "attributes": {"ApproximateReceiveCount": "1"},
                },
                {
                    "messageId": "3-4-5-6",
                    "receiptHandle": "receipt-3",
                    "eventSourceARN": "arn:aws:sqs:us-east-1:123456789:test-queue",
                    "body": json.dumps({"name": "charlie"}),
                    "attributes": {"ApproximateReceiveCount": "1"},
                },
            ]
        }
        sqs_handler = LambdaSqsStandard(
            clearskies.endpoints.Callable(mixed_handler), bindings={"sqs_client": mock_sqs_client_wrapper}
        )
        response = sqs_handler(records, {})

        # Should have no batch failures (retry was scheduled via visibility)
        self.assertEqual({}, response)

        # Should have called change_message_visibility once for bob
        mock_boto3_client.change_message_visibility.assert_called_once()
