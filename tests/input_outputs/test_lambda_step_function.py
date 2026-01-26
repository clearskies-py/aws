from __future__ import annotations

import unittest
from unittest.mock import MagicMock

from clearskies.di import Di
from clearskies.environment import Environment

from clearskies_aws.input_outputs.lambda_step_function import LambdaStepFunction


class LambdaStepFunctionInputOutputTest(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        # Environment requires an env_file_path, use empty string for tests
        self.environment = Environment("")
        self.di = MagicMock(spec=Di)
        # Make di.call_function just call the function with event kwarg
        self.di.call_function = lambda func, **kwargs: func(**kwargs)

    def test_inject_with_keys(self):
        """Test extracting environment variables with a list of keys."""
        event = {
            "Payload": {"group_id": 123},
            "BUSINESS_NAME": "TestBusiness",
            "API_KEY": "secret123",
            "OTHER_DATA": "ignored",
        }

        io = LambdaStepFunction(event, {}, environment_keys=["BUSINESS_NAME", "API_KEY"])
        io.inject_extra_environment_variables(self.environment, self.di)

        self.assertEqual(self.environment.get("BUSINESS_NAME"), "TestBusiness")
        self.assertEqual(self.environment.get("API_KEY"), "secret123")

    def test_inject_with_keys_missing_key_raises(self):
        """Test that missing keys raise KeyError."""
        event = {
            "BUSINESS_NAME": "TestBusiness",
        }

        io = LambdaStepFunction(event, {}, environment_keys=["BUSINESS_NAME", "MISSING_KEY"])

        with self.assertRaises(KeyError) as context:
            io.inject_extra_environment_variables(self.environment, self.di)

        self.assertIn("MISSING_KEY", str(context.exception))

    def test_inject_with_keys_none_value_allowed(self):
        """Test that None values are allowed and injected."""
        event = {
            "BUSINESS_NAME": "TestBusiness",
            "NULL_VALUE": None,
        }

        io = LambdaStepFunction(event, {}, environment_keys=["BUSINESS_NAME", "NULL_VALUE"])
        io.inject_extra_environment_variables(self.environment, self.di)

        self.assertEqual(self.environment.get("BUSINESS_NAME"), "TestBusiness")
        self.assertIsNone(self.environment.get("NULL_VALUE"))

    def test_inject_with_mapping(self):
        """Test extracting environment variables with a mapping dict."""
        event = {
            "BUSINESS_NAME": "TestBusiness",
            "GITLAB_AUTH_KEY": "/path/to/secret",
        }

        io = LambdaStepFunction(
            event,
            {},
            environment_keys={
                "BUSINESS_NAME": "COMPANY_NAME",
                "GITLAB_AUTH_KEY": "GITLAB_TOKEN_PATH",
            },
        )
        io.inject_extra_environment_variables(self.environment, self.di)

        self.assertEqual(self.environment.get("COMPANY_NAME"), "TestBusiness")
        self.assertEqual(self.environment.get("GITLAB_TOKEN_PATH"), "/path/to/secret")

    def test_inject_with_mapping_missing_key_raises(self):
        """Test that missing keys in mapping raise KeyError."""
        event = {
            "BUSINESS_NAME": "TestBusiness",
        }

        io = LambdaStepFunction(
            event,
            {},
            environment_keys={
                "BUSINESS_NAME": "COMPANY_NAME",
                "MISSING_KEY": "SOME_VALUE",
            },
        )

        with self.assertRaises(KeyError) as context:
            io.inject_extra_environment_variables(self.environment, self.di)

        self.assertIn("MISSING_KEY", str(context.exception))

    def test_inject_with_callable(self):
        """Test extracting environment variables with a callable."""
        event = {
            "nested": {"value": "deep"},
            "BUSINESS_NAME": "TestBusiness",
        }

        def extractor(event):
            return {
                "NESTED_VALUE": event.get("nested", {}).get("value"),
                "BUSINESS": event.get("BUSINESS_NAME"),
            }

        io = LambdaStepFunction(event, {}, environment_keys=extractor)
        io.inject_extra_environment_variables(self.environment, self.di)

        self.assertEqual(self.environment.get("NESTED_VALUE"), "deep")
        self.assertEqual(self.environment.get("BUSINESS"), "TestBusiness")

    def test_inject_with_callable_non_dict_raises(self):
        """Test that callable returning non-dict raises TypeError."""
        event = {"BUSINESS_NAME": "TestBusiness"}

        def extractor(event):
            return "not a dict"

        io = LambdaStepFunction(event, {}, environment_keys=extractor)

        with self.assertRaises(TypeError) as context:
            io.inject_extra_environment_variables(self.environment, self.di)

        self.assertIn("must return a dictionary", str(context.exception))

    def test_inject_with_callable_none_raises(self):
        """Test that callable returning None raises TypeError."""
        event = {"BUSINESS_NAME": "TestBusiness"}

        def extractor(event):
            return None

        io = LambdaStepFunction(event, {}, environment_keys=extractor)

        with self.assertRaises(TypeError) as context:
            io.inject_extra_environment_variables(self.environment, self.di)

        self.assertIn("must return a dictionary", str(context.exception))

    def test_inject_with_callable_uses_di(self):
        """Test that callable is called via DI to allow dependency injection."""
        event = {"BUSINESS_NAME": "TestBusiness"}
        call_args = {}

        def extractor(event, some_dependency=None):
            call_args["event"] = event
            call_args["some_dependency"] = some_dependency
            return {"BUSINESS": event.get("BUSINESS_NAME")}

        # Configure DI mock to pass through event and add a dependency
        self.di.call_function = lambda func, **kwargs: func(event=kwargs.get("event"), some_dependency="injected")

        io = LambdaStepFunction(event, {}, environment_keys=extractor)
        io.inject_extra_environment_variables(self.environment, self.di)

        self.assertEqual(call_args["event"], event)
        self.assertEqual(call_args["some_dependency"], "injected")
        self.assertEqual(self.environment.get("BUSINESS"), "TestBusiness")

    def test_inject_no_environment_keys(self):
        """Test that no injection happens when environment_keys is None."""
        event = {"BUSINESS_NAME": "TestBusiness"}

        io = LambdaStepFunction(event, {})
        io.inject_extra_environment_variables(self.environment, self.di)

        # Should not have set anything
        self.assertIsNone(self.environment.get("BUSINESS_NAME", silent=True))

    def test_context_specifics(self):
        """Test that context_specifics returns correct values."""
        event = {
            "BUSINESS_NAME": "TestBusiness",
            "$states": {"context": {"execution": {"id": "exec-123"}}},
        }
        context = {
            "function_name": "my-function",
            "function_version": "$LATEST",
            "aws_request_id": "req-456",
        }

        io = LambdaStepFunction(event, context, environment_keys=["BUSINESS_NAME"])

        specifics = io.context_specifics()

        self.assertEqual(specifics["invocation_type"], "step-functions")
        self.assertEqual(specifics["function_name"], "my-function")
        self.assertEqual(specifics["function_version"], "$LATEST")
        self.assertEqual(specifics["request_id"], "req-456")
        self.assertEqual(specifics["states_context"], {"context": {"execution": {"id": "exec-123"}}})
        self.assertEqual(specifics["event"], event)
        self.assertEqual(specifics["context"], context)
        # extracted_environment is no longer in context_specifics
        self.assertNotIn("extracted_environment", specifics)

    def test_context_specifics_without_states(self):
        """Test context_specifics when $states is not in event."""
        event = {"BUSINESS_NAME": "TestBusiness"}

        io = LambdaStepFunction(event, {})

        specifics = io.context_specifics()

        self.assertEqual(specifics["states_context"], {})

    def test_get_protocol(self):
        """Test that protocol is step-functions."""
        io = LambdaStepFunction({}, {})
        self.assertEqual(io.get_protocol(), "step-functions")

    def test_has_body(self):
        """Test that has_body always returns True."""
        io = LambdaStepFunction({}, {})
        self.assertTrue(io.has_body())

    def test_get_body_returns_json_string(self):
        """Test that get_body returns JSON-encoded event."""
        event = {"key": "value", "nested": {"data": 123}}
        io = LambdaStepFunction(event, {})
        import json

        self.assertEqual(io.get_body(), json.dumps(event))

    def test_get_client_ip(self):
        """Test that get_client_ip returns localhost."""
        io = LambdaStepFunction({}, {})
        self.assertEqual(io.get_client_ip(), "127.0.0.1")

    def test_respond_returns_body_directly(self):
        """Test that respond returns body unchanged."""
        io = LambdaStepFunction({}, {})
        body = {"result": "success"}
        self.assertEqual(io.respond(body), body)

    def test_respond_decodes_bytes(self):
        """Test that respond decodes bytes to string."""
        io = LambdaStepFunction({}, {})
        body = b"binary data"
        self.assertEqual(io.respond(body), "binary data")

    def test_request_data_returns_event(self):
        """Test that request_data returns the event."""
        event = {"key": "value"}
        io = LambdaStepFunction(event, {})
        self.assertEqual(io.request_data, event)

    def test_json_body_returns_event(self):
        """Test that json_body returns the event."""
        event = {"key": "value"}
        io = LambdaStepFunction(event, {})
        self.assertEqual(io.json_body(), event)

    def test_json_body_raises_on_empty_event_when_required(self):
        """Test that json_body raises ClientError on empty event when required."""
        io = LambdaStepFunction({}, {})
        from clearskies.exceptions import ClientError

        with self.assertRaises(ClientError):
            io.json_body(required=True)

    def test_json_body_returns_empty_when_not_required(self):
        """Test that json_body returns empty dict when not required."""
        io = LambdaStepFunction({}, {})
        self.assertEqual(io.json_body(required=False), {})

    def test_url_handling(self):
        """Test URL is set correctly."""
        io = LambdaStepFunction({}, {}, url="my/path")
        self.assertEqual(io.url, "my/path")
        self.assertEqual(io.path, "my/path")

    def test_request_method_handling(self):
        """Test request method is uppercased."""
        io = LambdaStepFunction({}, {}, request_method="post")
        self.assertEqual(io.request_method, "POST")

    def test_no_url_sets_supports_url(self):
        """Test that supports_url is True when no URL provided."""
        io = LambdaStepFunction({}, {})
        self.assertTrue(io.supports_url)

    def test_no_request_method_sets_supports_request_method_false(self):
        """Test that supports_request_method is False when no method provided."""
        io = LambdaStepFunction({}, {})
        self.assertFalse(io.supports_request_method)

    def test_values_not_converted_to_strings(self):
        """Test that values are NOT converted to strings (per maintainer feedback)."""
        event = {
            "INT_VALUE": 123,
            "FLOAT_VALUE": 45.67,
            "BOOL_VALUE": True,
        }

        io = LambdaStepFunction(event, {}, environment_keys=["INT_VALUE", "FLOAT_VALUE", "BOOL_VALUE"])
        io.inject_extra_environment_variables(self.environment, self.di)

        # Values should retain their original types
        self.assertEqual(self.environment.get("INT_VALUE"), 123)
        self.assertEqual(self.environment.get("FLOAT_VALUE"), 45.67)
        self.assertEqual(self.environment.get("BOOL_VALUE"), True)
