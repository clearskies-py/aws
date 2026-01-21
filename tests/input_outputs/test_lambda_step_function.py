from __future__ import annotations

import unittest

from clearskies_aws.input_outputs.lambda_step_function import LambdaStepFunction


class LambdaStepFunctionInputOutputTest(unittest.TestCase):
    def test_extract_with_keys(self):
        event = {
            "Payload": {"group_id": 123},
            "BUSINESS_NAME": "TestBusiness",
            "API_KEY": "secret123",
            "OTHER_DATA": "ignored",
        }

        io = LambdaStepFunction(event, {}, environment_keys=["BUSINESS_NAME", "API_KEY"])

        self.assertEqual(
            io.extracted_environment,
            {
                "BUSINESS_NAME": "TestBusiness",
                "API_KEY": "secret123",
            },
        )

    def test_extract_with_keys_missing_key(self):
        event = {
            "BUSINESS_NAME": "TestBusiness",
        }

        io = LambdaStepFunction(event, {}, environment_keys=["BUSINESS_NAME", "MISSING_KEY"])

        self.assertEqual(
            io.extracted_environment,
            {
                "BUSINESS_NAME": "TestBusiness",
            },
        )

    def test_extract_with_keys_none_value(self):
        event = {
            "BUSINESS_NAME": "TestBusiness",
            "NULL_VALUE": None,
        }

        io = LambdaStepFunction(event, {}, environment_keys=["BUSINESS_NAME", "NULL_VALUE"])

        self.assertEqual(
            io.extracted_environment,
            {
                "BUSINESS_NAME": "TestBusiness",
            },
        )

    def test_extract_with_mapping(self):
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

        self.assertEqual(
            io.extracted_environment,
            {
                "COMPANY_NAME": "TestBusiness",
                "GITLAB_TOKEN_PATH": "/path/to/secret",
            },
        )

    def test_extract_with_mapping_missing_key(self):
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

        self.assertEqual(
            io.extracted_environment,
            {
                "COMPANY_NAME": "TestBusiness",
            },
        )

    def test_extract_with_callable(self):
        event = {
            "nested": {"value": "deep"},
            "BUSINESS_NAME": "TestBusiness",
        }

        def extractor(evt):
            return {
                "NESTED_VALUE": evt.get("nested", {}).get("value"),
                "BUSINESS": evt.get("BUSINESS_NAME"),
            }

        io = LambdaStepFunction(event, {}, environment_keys=extractor)

        self.assertEqual(
            io.extracted_environment,
            {
                "NESTED_VALUE": "deep",
                "BUSINESS": "TestBusiness",
            },
        )

    def test_extract_with_callable_returns_none(self):
        event = {"BUSINESS_NAME": "TestBusiness"}

        def extractor(evt):
            return None

        io = LambdaStepFunction(event, {}, environment_keys=extractor)

        self.assertEqual(io.extracted_environment, {})

    def test_extract_with_callable_filters_none_values(self):
        event = {"BUSINESS_NAME": "TestBusiness"}

        def extractor(evt):
            return {
                "BUSINESS": evt.get("BUSINESS_NAME"),
                "MISSING": evt.get("MISSING_KEY"),
            }

        io = LambdaStepFunction(event, {}, environment_keys=extractor)

        self.assertEqual(
            io.extracted_environment,
            {
                "BUSINESS": "TestBusiness",
            },
        )

    def test_context_specifics(self):
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
        self.assertEqual(specifics["extracted_environment"], {"BUSINESS_NAME": "TestBusiness"})
        self.assertEqual(specifics["event"], event)
        self.assertEqual(specifics["context"], context)

    def test_context_specifics_without_states(self):
        event = {"BUSINESS_NAME": "TestBusiness"}

        io = LambdaStepFunction(event, {})

        specifics = io.context_specifics()

        self.assertEqual(specifics["states_context"], {})

    def test_get_protocol(self):
        io = LambdaStepFunction({}, {})
        self.assertEqual(io.get_protocol(), "step-functions")

    def test_has_body(self):
        io = LambdaStepFunction({}, {})
        self.assertTrue(io.has_body())

    def test_get_body_returns_json_string(self):
        event = {"key": "value", "nested": {"data": 123}}
        io = LambdaStepFunction(event, {})
        # get_body returns JSON string for consistency with parent class
        import json

        self.assertEqual(io.get_body(), json.dumps(event))

    def test_get_client_ip(self):
        io = LambdaStepFunction({}, {})
        self.assertEqual(io.get_client_ip(), "127.0.0.1")

    def test_respond_returns_body_directly(self):
        io = LambdaStepFunction({}, {})
        body = {"result": "success"}
        self.assertEqual(io.respond(body), body)

    def test_respond_decodes_bytes(self):
        io = LambdaStepFunction({}, {})
        body = b"binary data"
        self.assertEqual(io.respond(body), "binary data")

    def test_request_data_returns_event(self):
        event = {"key": "value"}
        io = LambdaStepFunction(event, {})
        self.assertEqual(io.request_data, event)

    def test_json_body_returns_event(self):
        event = {"key": "value"}
        io = LambdaStepFunction(event, {})
        self.assertEqual(io.json_body(), event)

    def test_json_body_raises_on_empty_event_when_required(self):
        io = LambdaStepFunction({}, {})
        # Empty dict is falsy, so it should raise
        from clearskies.exceptions import ClientError

        with self.assertRaises(ClientError):
            io.json_body(required=True)

    def test_json_body_returns_none_when_not_required(self):
        io = LambdaStepFunction({}, {})
        # Empty dict is falsy, but not required so returns the empty dict
        self.assertEqual(io.json_body(required=False), {})

    def test_url_handling(self):
        io = LambdaStepFunction({}, {}, url="my/path")
        self.assertEqual(io.url, "my/path")
        self.assertEqual(io.path, "my/path")

    def test_request_method_handling(self):
        io = LambdaStepFunction({}, {}, request_method="post")
        self.assertEqual(io.request_method, "POST")

    def test_no_url_sets_supports_url(self):
        io = LambdaStepFunction({}, {})
        self.assertTrue(io.supports_url)

    def test_no_request_method_sets_supports_request_method_false(self):
        io = LambdaStepFunction({}, {})
        self.assertFalse(io.supports_request_method)

    def test_values_converted_to_strings(self):
        event = {
            "INT_VALUE": 123,
            "FLOAT_VALUE": 45.67,
            "BOOL_VALUE": True,
        }

        io = LambdaStepFunction(event, {}, environment_keys=["INT_VALUE", "FLOAT_VALUE", "BOOL_VALUE"])

        self.assertEqual(
            io.extracted_environment,
            {
                "INT_VALUE": "123",
                "FLOAT_VALUE": "45.67",
                "BOOL_VALUE": "True",
            },
        )
