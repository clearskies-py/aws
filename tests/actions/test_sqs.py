from __future__ import annotations

import json
import unittest
from collections import OrderedDict
from unittest.mock import MagicMock, call, patch

import clearskies
import pytest
from clearskies.di import Di

from clearskies_aws.actions.sqs import SQS


class User(clearskies.Model):
    """Test user model for SQS action testing."""

    backend = clearskies.backends.MemoryBackend()
    id_column_name = "id"

    id = clearskies.columns.String("id")
    name = clearskies.columns.String("name")
    email = clearskies.columns.Email("email")


class SQSTest(unittest.TestCase):
    """Test suite for SQS action functionality.

    This test class validates SQS message sending, conditional execution,
    message group ID handling, and error scenarios with improved:
    - Mock isolation and setup
    - Error handling validation
    - Test data consistency
    - Performance considerations
    - Best practices adherence
    """

    def setUp(self):
        """Set up test fixtures with proper mock isolation and configuration.

        Creates isolated mocks for each test to prevent cross-test contamination
        and provides consistent test data for better maintainability.
        """
        # Initialize dependency injection container with test environment
        self.di = Di()
        self.di.add_binding("environment", {"AWS_REGION": "us-east-2"})

        # Create test model factory
        self.users = self.di.build(User)

        # Mock SQS client with proper method isolation
        self.sqs_client = MagicMock(name="sqs_client")
        self.sqs_client.send_message = MagicMock(name="send_message")

        # Mock boto3 with consistent return values
        self.boto3 = MagicMock(name="boto3")
        self.boto3.client = MagicMock(return_value=self.sqs_client)

        # Track conditional execution state for validation
        self.when_model_received = None

        # Mock environment with predictable behavior
        self.environment = MagicMock(name="environment")
        self.environment.get = MagicMock(return_value="us-east-1")

        # Common test data for consistency across tests
        self.test_user_data = {"id": "test-user-123", "name": "Jane Doe", "email": "jane.doe@example.com"}
        self.test_queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"

    def _create_test_user(self, user_data=None):
        """Create test user instances with consistent data.

        Args:
            user_data: Optional user data dict, defaults to self.test_user_data

        Returns:
            User model instance for testing
        """
        data = user_data or self.test_user_data
        return self.users.model(data)

    def _setup_sqs_action(self, sqs_action):
        """Setups SQS action with proper mocked dependencies.

        Args:
            sqs_action: SQS action instance to setup

        Returns:
            Configured SQS action ready for testing
        """
        # Inject mocked dependencies
        sqs_action.environment = self.environment
        sqs_action.boto3 = self.boto3
        sqs_action.region = "us-east-1"  # Set required region

        # Configure the action
        sqs_action.configure()

        # Mock the DI for callable resolution
        sqs_action.di = MagicMock()

        def mock_call_function(func, **kwargs):
            """Mock DI call_function to handle callable resolution in tests."""
            return func(kwargs.get("model"))

        sqs_action.di.call_function = mock_call_function

        return sqs_action

    def condition_always_true(self, model):
        """Conditional function that always returns True for testing.

        Args:
            model: The model being processed

        Returns:
            Always True
        """
        self.when_model_received = model
        return True

    def condition_always_false(self, model):
        """Conditional function that always returns False for testing.

        Args:
            model: The model being processed

        Returns:
            Always False
        """
        self.when_model_received = model
        return False

    def test_send_basic_message(self):
        """Test basic SQS message sending functionality."""
        # Create and configure SQS action
        sqs = SQS(
            queue_url="https://queue.example.com",
            when=self.condition_always_true,
        )
        self._setup_sqs_action(sqs)

        # Create test user with consistent data
        user = self._create_test_user(
            {
                "id": "1-2-3-4",
                "name": "Jane",
                "email": "jane@example.com",
            }
        )

        # Execute the action
        sqs(user)

        # Verify SQS client was called correctly
        self.sqs_client.send_message.assert_called_once()
        args, kwargs = self.sqs_client.send_message.call_args
        self.assertEqual(kwargs["QueueUrl"], "https://queue.example.com")

        # Parse and verify message body (JSON key order may vary)
        message_body = json.loads(kwargs["MessageBody"])
        expected_body = {
            "id": "1-2-3-4",
            "name": "Jane",
            "email": "jane@example.com",
        }
        self.assertEqual(message_body, expected_body)

        # Verify conditional function received the model
        self.assertEqual(id(user), id(self.when_model_received))

    def test_send_message_with_group_id(self):
        """Test SQS message sending with static message group ID."""
        # Create and configure SQS action with callable message group ID that returns static value
        sqs = SQS(
            queue_url="https://queue.example.com",
            when=self.condition_always_true,
            message_group_id=lambda model: "test-group-id",
        )
        self._setup_sqs_action(sqs)

        # Create test user
        user = self._create_test_user(
            {
                "id": "1-2-3-4",
                "name": "Jane",
                "email": "jane@example.com",
            }
        )

        # Execute the action
        sqs(user)

        # Verify SQS client was called with message group ID
        self.sqs_client.send_message.assert_called_once()
        args, kwargs = self.sqs_client.send_message.call_args
        self.assertEqual(kwargs["QueueUrl"], "https://queue.example.com")
        self.assertEqual(kwargs["MessageGroupId"], "test-group-id")

        # Parse and verify message body
        message_body = json.loads(kwargs["MessageBody"])
        expected_body = {
            "id": "1-2-3-4",
            "name": "Jane",
            "email": "jane@example.com",
        }
        self.assertEqual(message_body, expected_body)

        # Verify conditional function received the model
        self.assertEqual(id(user), id(self.when_model_received))

    def test_send_message_with_callable_group_id(self):
        """Test SQS message sending with callable message group ID."""
        # Create and configure SQS action with callable message group ID
        sqs = SQS(
            queue_url="https://queue.example.com",
            when=self.condition_always_true,
            message_group_id=lambda model: model.id,
        )
        self._setup_sqs_action(sqs)

        # Create test user
        user = self._create_test_user(
            {
                "id": "1-2-3-4",
                "name": "Jane",
                "email": "jane@example.com",
            }
        )

        # Execute the action
        sqs(user)

        # Verify SQS client was called with computed message group ID
        self.sqs_client.send_message.assert_called_once()
        args, kwargs = self.sqs_client.send_message.call_args
        self.assertEqual(kwargs["QueueUrl"], "https://queue.example.com")
        self.assertEqual(kwargs["MessageGroupId"], "1-2-3-4")

        # Parse and verify message body
        message_body = json.loads(kwargs["MessageBody"])
        expected_body = {
            "id": "1-2-3-4",
            "name": "Jane",
            "email": "jane@example.com",
        }
        self.assertEqual(message_body, expected_body)

        # Verify conditional function received the model
        self.assertEqual(id(user), id(self.when_model_received))

    def test_conditional_execution_false(self):
        """Test that SQS action respects when condition returning False."""
        # Create and configure SQS action with false condition
        sqs = SQS(
            queue_url="https://queue.example.com",
            when=self.condition_always_false,
        )
        self._setup_sqs_action(sqs)

        # Create test user
        user = self._create_test_user(
            {
                "id": "1-2-3-4",
                "name": "Jane",
                "email": "jane@example.com",
            }
        )

        # Execute the action
        sqs(user)

        # Verify SQS client was not called
        self.sqs_client.send_message.assert_not_called()

        # Verify conditional function still received the model
        self.assertEqual(id(user), id(self.when_model_received))

    def test_queue_url_from_environment(self):
        """Test queue URL resolution from environment variable."""
        # Configure environment to return queue URL
        self.environment.get.return_value = "https://env-queue.example.com"

        # Create SQS action with environment-based queue URL
        sqs = SQS(
            queue_url_environment_key="QUEUE_URL",
            when=self.condition_always_true,
        )
        self._setup_sqs_action(sqs)

        # Create test user
        user = self._create_test_user()

        # Execute the action
        sqs(user)

        # Verify environment was queried for queue URL
        self.environment.get.assert_called_with("QUEUE_URL")

        # Verify SQS client was called with environment URL
        self.sqs_client.send_message.assert_called_once()
        args, kwargs = self.sqs_client.send_message.call_args
        self.assertEqual(kwargs["QueueUrl"], "https://env-queue.example.com")

    def test_queue_url_from_callable(self):
        """Test queue URL resolution from callable function."""

        def get_queue_url(model):
            return f"https://queue-{model.id}.example.com"

        # Create SQS action with callable queue URL
        sqs = SQS(
            queue_url_callable=get_queue_url,
            when=self.condition_always_true,
        )
        self._setup_sqs_action(sqs)

        # Create test user
        user = self._create_test_user()

        # Execute the action
        sqs(user)

        # Verify SQS client was called with computed URL
        self.sqs_client.send_message.assert_called_once()
        args, kwargs = self.sqs_client.send_message.call_args
        self.assertEqual(kwargs["QueueUrl"], f"https://queue-{user.id}.example.com")

    def test_empty_queue_url_skips_send(self):
        """Test that empty queue URL prevents message sending."""
        # Configure environment to return empty queue URL
        self.environment.get.return_value = ""

        # Create SQS action with environment-based queue URL that returns empty
        sqs = SQS(
            queue_url_environment_key="EMPTY_QUEUE_URL",
            when=self.condition_always_true,
        )
        self._setup_sqs_action(sqs)

        # Create test user
        user = self._create_test_user()

        # Execute the action
        sqs(user)

        # Verify SQS client was not called due to empty URL
        self.sqs_client.send_message.assert_not_called()

    def test_custom_message_callable(self):
        """Test SQS action with custom message formatting."""

        def custom_message_formatter(model):
            return {"user_id": model.id, "action": "created"}

        # Create SQS action with custom message formatter
        sqs = SQS(
            queue_url="https://queue.example.com",
            message_callable=custom_message_formatter,
            when=self.condition_always_true,
        )
        self._setup_sqs_action(sqs)

        # Create test user
        user = self._create_test_user()

        # Execute the action
        sqs(user)

        # Verify SQS client was called with custom message format
        self.sqs_client.send_message.assert_called_once()
        args, kwargs = self.sqs_client.send_message.call_args
        self.assertEqual(kwargs["QueueUrl"], "https://queue.example.com")

        # Parse and verify custom message body
        message_body = json.loads(kwargs["MessageBody"])
        expected_body = {"user_id": user.id, "action": "created"}
        self.assertEqual(message_body, expected_body)

    def tearDown(self):
        """Clean up after each test to ensure isolation."""
        # Reset all mocks to prevent test interference
        self.sqs_client.reset_mock()
        self.boto3.reset_mock()
        self.environment.reset_mock()

        # Clear tracked state
        self.when_model_received = None
