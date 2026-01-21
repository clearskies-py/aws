from __future__ import annotations

import unittest

import clearskies

from clearskies_aws.contexts.lambda_step_function import LambdaStepFunction


class LambdaStepFunctionContextTest(unittest.TestCase):
    def setUp(self):
        clearskies.backends.MemoryBackend.clear_table_cache()

    def test_basic_invocation(self):
        def my_function(request_data):
            return request_data

        application = LambdaStepFunction(
            clearskies.endpoints.Callable(
                my_function,
                return_standard_response=False,
            )
        )

        response = application(
            {"key": "value"},
            {},
        )

        self.assertEqual(response, {"key": "value"})

    def test_with_environment_keys(self):
        """Test that environment keys are extracted and available via environment.get()."""
        captured_env = {}

        def my_function(request_data, environment):
            captured_env["BUSINESS_NAME"] = environment.get("BUSINESS_NAME", silent=True)
            return request_data

        application = LambdaStepFunction(
            clearskies.endpoints.Callable(
                my_function,
                return_standard_response=False,
            ),
            environment_keys=["BUSINESS_NAME"],
        )

        application(
            {"BUSINESS_NAME": "TestBusiness", "data": "value"},
            {},
        )

        self.assertEqual(captured_env["BUSINESS_NAME"], "TestBusiness")

    def test_with_environment_mapping(self):
        """Test that environment mapping renames keys correctly."""
        captured_env = {}

        def my_function(request_data, environment):
            captured_env["COMPANY_NAME"] = environment.get("COMPANY_NAME", silent=True)
            return request_data

        application = LambdaStepFunction(
            clearskies.endpoints.Callable(
                my_function,
                return_standard_response=False,
            ),
            environment_keys={"BUSINESS_NAME": "COMPANY_NAME"},
        )

        application(
            {"BUSINESS_NAME": "TestBusiness"},
            {},
        )

        self.assertEqual(captured_env["COMPANY_NAME"], "TestBusiness")

    def test_with_environment_callable(self):
        """Test that environment callable extracts values correctly."""
        captured_env = {}

        def extract_env(event):
            return {
                "NESTED_VALUE": event.get("nested", {}).get("value"),
            }

        def my_function(request_data, environment):
            captured_env["NESTED_VALUE"] = environment.get("NESTED_VALUE", silent=True)
            return request_data

        application = LambdaStepFunction(
            clearskies.endpoints.Callable(
                my_function,
                return_standard_response=False,
            ),
            environment_keys=extract_env,
        )

        application(
            {"nested": {"value": "deep_value"}},
            {},
        )

        self.assertEqual(captured_env["NESTED_VALUE"], "deep_value")

    def test_context_specifics_available(self):
        """Test that context specifics are available for injection."""
        captured_specifics = {}

        def my_function(request_data, invocation_type, states_context, extracted_environment):
            captured_specifics["invocation_type"] = invocation_type
            captured_specifics["states_context"] = states_context
            captured_specifics["extracted_environment"] = extracted_environment
            return request_data

        application = LambdaStepFunction(
            clearskies.endpoints.Callable(
                my_function,
                return_standard_response=False,
            ),
            environment_keys=["BUSINESS_NAME"],
        )

        application(
            {
                "BUSINESS_NAME": "TestBusiness",
                "$states": {"context": {"execution": {"id": "exec-123"}}},
            },
            {},
        )

        self.assertEqual(captured_specifics["invocation_type"], "step-functions")
        self.assertEqual(captured_specifics["states_context"], {"context": {"execution": {"id": "exec-123"}}})
        self.assertEqual(captured_specifics["extracted_environment"], {"BUSINESS_NAME": "TestBusiness"})

    def test_no_environment_extraction(self):
        """Test that environment.get() returns None when no extraction is configured."""
        captured_env = {}

        def my_function(request_data, environment):
            captured_env["BUSINESS_NAME"] = environment.get("BUSINESS_NAME", silent=True)
            return request_data

        application = LambdaStepFunction(
            clearskies.endpoints.Callable(
                my_function,
                return_standard_response=False,
            ),
            # No environment_keys
        )

        application(
            {"BUSINESS_NAME": "TestBusiness"},
            {},
        )

        # Should be None since we didn't configure extraction
        self.assertIsNone(captured_env["BUSINESS_NAME"])

    def test_multiple_invocations_independent(self):
        """Test that multiple invocations have independent extracted environments."""
        captured_values = []

        def my_function(request_data, environment):
            captured_values.append(environment.get("VALUE", silent=True))
            return request_data

        application = LambdaStepFunction(
            clearskies.endpoints.Callable(
                my_function,
                return_standard_response=False,
            ),
            environment_keys=["VALUE"],
        )

        application({"VALUE": "first"}, {})
        application({"VALUE": "second"}, {})

        self.assertEqual(captured_values, ["first", "second"])
