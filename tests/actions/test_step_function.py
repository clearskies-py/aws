from __future__ import annotations

import json
import unittest
from collections import OrderedDict
from unittest.mock import MagicMock, call

import clearskies
import pytest
from clearskies.di import Di

from clearskies_aws.actions.step_function import StepFunction


class User(clearskies.Model):
    """Test user model for Step Function action testing."""

    backend = clearskies.backends.MemoryBackend()
    id_column_name = "id"

    id = clearskies.columns.String("id")
    email = clearskies.columns.Email("email")
    execution_arn = clearskies.columns.String("execution_arn")


class StepFunctionTest(unittest.TestCase):
    """Test suite for Step Function action functionality.

    This test class validates Step Function execution, regional switching,
    conditional execution, and execution ARN storage with improved:
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

        # Mock Step Functions client with proper method isolation
        self.step_function_client = MagicMock(name="step_function_client")
        self.step_function_client.start_execution = MagicMock(
            name="start_execution",
            return_value={"executionArn": "arn:aws:states:us-east-1:123456789012:execution:test-execution"},
        )

        # Mock boto3 with consistent return values
        self.boto3 = MagicMock(name="boto3")
        self.boto3.client = MagicMock(return_value=self.step_function_client)

        # Track conditional execution state for validation
        self.when_model_received = None

        # Mock environment with predictable behavior
        self.environment = MagicMock(name="environment")
        self.environment.get = MagicMock(return_value="us-east-2")

        # Common test data for consistency across tests
        self.test_user_data = {"id": "test-user-456", "email": "test.user@example.com"}
        self.test_arn = "arn:aws:states:us-east-1:123456789012:stateMachine:test-state-machine"

    def _create_test_user(self, user_data=None):
        """Create test user instances with consistent data.

        Args:
            user_data: Optional user data dict, defaults to self.test_user_data

        Returns:
            User model instance for testing
        """
        data = user_data or self.test_user_data
        return self.users.model(data)

    def _setup_step_function_action(self, step_function_action):
        """Setups Step Function action with proper mocked dependencies.

        Args:
            step_function_action: Step Function action instance to setup

        Returns:
            Configured Step Function action ready for testing
        """
        # Inject mocked dependencies
        step_function_action.environment = self.environment
        step_function_action.boto3 = self.boto3
        step_function_action.region = "us-east-2"  # Set required region

        # Configure the action
        step_function_action.configure()

        # Mock the DI for callable resolution
        step_function_action.di = MagicMock()

        def mock_call_function(func, **kwargs):
            """Mock DI call_function to handle callable resolution in tests."""
            return func(kwargs.get("model"))

        step_function_action.di.call_function = mock_call_function

        return step_function_action

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

    def test_execute_basic_step_function(self):
        """Test basic Step Function execution functionality."""
        # Create and configure Step Function action
        step_function = StepFunction(
            arn=self.test_arn,
            when=self.condition_always_true,
        )
        self._setup_step_function_action(step_function)

        # Create test user
        user = self._create_test_user(
            {
                "id": "1-2-3-4",
                "email": "jane@example.com",
            }
        )

        # Execute the action
        step_function(user)

        # Verify Step Function client was called correctly
        self.step_function_client.start_execution.assert_called_once()
        args, kwargs = self.step_function_client.start_execution.call_args
        self.assertEqual(kwargs["stateMachineArn"], self.test_arn)

        # Parse and verify message body (JSON key order may vary)
        message_body = json.loads(kwargs["input"])
        expected_body = {
            "id": "1-2-3-4",
            "email": "jane@example.com",
            "execution_arn": None,  # Include all model fields
        }
        self.assertEqual(message_body, expected_body)

        # Verify conditional function received the model
        self.assertEqual(id(user), id(self.when_model_received))

    def test_execute_with_execution_arn_storage(self):
        """Test Step Function execution with execution ARN storage."""
        # Create and configure Step Function action with execution ARN storage
        step_function = StepFunction(
            arn=self.test_arn,
            when=self.condition_always_true,
            column_to_store_execution_arn="execution_arn",
        )
        self._setup_step_function_action(step_function)

        # Create test user
        user = self._create_test_user(
            {
                "id": "1-2-3-4",
                "email": "jane@example.com",
            }
        )

        # Mock the save method to track execution ARN storage
        user.save = MagicMock()

        # Execute the action
        step_function(user)

        # Verify Step Function client was called
        self.step_function_client.start_execution.assert_called_once()

        # Verify execution ARN was stored
        user.save.assert_called_once_with(
            {"execution_arn": "arn:aws:states:us-east-1:123456789012:execution:test-execution"}
        )

        # Verify conditional function received the model
        self.assertEqual(id(user), id(self.when_model_received))

    def test_region_switching(self):
        """Test Step Function region switching based on ARN region."""
        # Create Step Function action with different region ARN
        eu_arn = "arn:aws:states:eu-west-1:123456789012:stateMachine:test-state-machine"

        step_function = StepFunction(
            arn=eu_arn,
            when=self.condition_always_true,
        )

        # Setup with region switching behavior
        step_function.environment = self.environment
        step_function.boto3 = self.boto3
        step_function.region = "us-east-2"  # Initial region
        step_function.configure()

        # Mock DI
        step_function.di = MagicMock()
        step_function.di.call_function = lambda func, **kwargs: func(kwargs.get("model"))

        # Create test user
        user = self._create_test_user()

        # Execute the action
        step_function(user)

        # Verify region was switched to eu-west-1 from the ARN
        self.assertEqual(step_function.region, "eu-west-1")

        # Verify Step Function was called with correct ARN
        self.step_function_client.start_execution.assert_called_once()
        args, kwargs = self.step_function_client.start_execution.call_args
        self.assertEqual(kwargs["stateMachineArn"], eu_arn)

    def test_conditional_execution_false(self):
        """Test that Step Function action respects when condition returning False."""
        # Create and configure Step Function action with false condition
        step_function = StepFunction(
            arn=self.test_arn,
            when=self.condition_always_false,
        )
        self._setup_step_function_action(step_function)

        # Create test user
        user = self._create_test_user(
            {
                "id": "1-2-3-4",
                "email": "jane@example.com",
            }
        )

        # Execute the action
        step_function(user)

        # Verify Step Function client was not called
        self.step_function_client.start_execution.assert_not_called()

        # Verify conditional function still received the model
        self.assertEqual(id(user), id(self.when_model_received))

    def test_arn_from_environment(self):
        """Test ARN resolution from environment variable."""
        # Configure environment to return ARN
        env_arn = "arn:aws:states:us-west-2:123456789012:stateMachine:env-state-machine"
        self.environment.get.return_value = env_arn

        # Create Step Function action with environment-based ARN
        step_function = StepFunction(
            arn_environment_key="STEP_FUNCTION_ARN",
            when=self.condition_always_true,
        )
        self._setup_step_function_action(step_function)

        # Create test user
        user = self._create_test_user()

        # Execute the action
        step_function(user)

        # Verify environment was queried for ARN
        self.environment.get.assert_called_with("STEP_FUNCTION_ARN")

        # Verify Step Function client was called with environment ARN
        self.step_function_client.start_execution.assert_called_once()
        args, kwargs = self.step_function_client.start_execution.call_args
        self.assertEqual(kwargs["stateMachineArn"], env_arn)

    def test_arn_from_callable(self):
        """Test ARN resolution from callable function."""

        def get_step_function_arn(model):
            return f"arn:aws:states:us-east-1:123456789012:stateMachine:user-{model.id}-workflow"

        # Create Step Function action with callable ARN
        step_function = StepFunction(
            arn_callable=get_step_function_arn,
            when=self.condition_always_true,
        )
        self._setup_step_function_action(step_function)

        # Create test user
        user = self._create_test_user()

        # Execute the action
        step_function(user)

        # Verify Step Function client was called with computed ARN
        self.step_function_client.start_execution.assert_called_once()
        args, kwargs = self.step_function_client.start_execution.call_args
        expected_arn = f"arn:aws:states:us-east-1:123456789012:stateMachine:user-{user.id}-workflow"
        self.assertEqual(kwargs["stateMachineArn"], expected_arn)

    def test_custom_message_callable(self):
        """Test Step Function action with custom message formatting."""

        def custom_message_formatter(model):
            return {"user_id": model.id, "action": "workflow_started", "timestamp": "2023-01-01T00:00:00Z"}

        # Create Step Function action with custom message formatter
        step_function = StepFunction(
            arn=self.test_arn,
            message_callable=custom_message_formatter,
            when=self.condition_always_true,
        )
        self._setup_step_function_action(step_function)

        # Create test user
        user = self._create_test_user()

        # Execute the action
        step_function(user)

        # Verify Step Function client was called with custom message format
        self.step_function_client.start_execution.assert_called_once()
        args, kwargs = self.step_function_client.start_execution.call_args
        self.assertEqual(kwargs["stateMachineArn"], self.test_arn)

        # Parse and verify custom message body
        message_body = json.loads(kwargs["input"])
        expected_body = {"user_id": user.id, "action": "workflow_started", "timestamp": "2023-01-01T00:00:00Z"}
        self.assertEqual(message_body, expected_body)

    def tearDown(self):
        """Clean up after each test to ensure isolation."""
        # Reset all mocks to prevent test interference
        self.step_function_client.reset_mock()
        self.boto3.reset_mock()
        self.environment.reset_mock()

        # Clear tracked state
        self.when_model_received = None
