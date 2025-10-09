from __future__ import annotations

import unittest

from clearskies.input_outputs import Headers

from clearskies_aws.input_outputs import LambdaAPIGateway


class LambdaAPIGatewayTest(unittest.TestCase):
    request_headers = Headers({"Content-Type": "application/json; charset=UTF-8"})

    dummy_event_v1 = {
        "httpMethod": "GET",
        "path": "/test",
        "resource": "/test/{id}",
        "queryStringParameters": {"q": "hey", "bob": "sup"},
        "multiValueQueryStringParameters": {"tags": ["urgent", "important"]},
        "pathParameters": {"id": "123"},
        "headers": {"Content-Type": "application/json"},
        "multiValueHeaders": {"X-Custom": ["value1", "value2"]},
        "requestContext": {
            "stage": "prod",
            "requestId": "test-request-id",
            "apiId": "test-api-id",
            "identity": {"sourceIp": "192.168.1.1"},
        },
    }

    dummy_event_v2 = {
        "version": "2.0",
        "requestContext": {
            "http": {
                "method": "GET",
                "path": "/test",
                "protocol": "HTTP/1.1",
                "sourceIp": "192.168.1.2",
                "userAgent": "test-user-agent",
            },
            "stage": "dev",
            "requestId": "test-request-id-v2",
            "apiId": "test-api-id-v2",
            "domainName": "api.example.com",
        },
        "queryStringParameters": {"q": "hello", "name": "world"},
        "pathParameters": {"id": "456"},
        "headers": {"content-type": "application/json", "authorization": "Bearer token"},
    }

    def test_respond_v1(self):
        """Test response format for API Gateway v1."""
        response_headers = Headers({"Content-Type": "application/json; charset=UTF-8"})
        aws_lambda = LambdaAPIGateway(self.dummy_event_v1, {})

        response = aws_lambda.respond({"some": "data"}, 200)
        self.assertEqual(
            {
                "isBase64Encoded": False,
                "statusCode": 200,
                "headers": response_headers,
                "body": '{"some": "data"}',
            },
            response,
        )

    def test_respond_v2(self):
        """Test response format for API Gateway v2."""
        response_headers = Headers({"Content-Type": "application/json; charset=UTF-8"})
        aws_lambda = LambdaAPIGateway(self.dummy_event_v2, {})

        response = aws_lambda.respond({"some": "data"}, 200)
        self.assertEqual(
            {
                "isBase64Encoded": False,
                "statusCode": 200,
                "headers": response_headers,
                "body": '{"some": "data"}',
            },
            response,
        )

    def test_headers_v1(self):
        """Test header parsing for API Gateway v1."""
        aws_lambda = LambdaAPIGateway(
            {
                **self.dummy_event_v1,
                **{
                    "headers": {
                        "Content-Type": "application/json",
                        "AUTHORIZATION": "hey",
                        "X-Auth": "asdf",
                    },
                    "multiValueHeaders": {
                        "X-Custom": ["value1", "value2"],
                    },
                },
            },
            {},
        )
        self.assertEqual("hey", aws_lambda.request_headers.get("authorizatiON"))
        self.assertEqual("asdf", aws_lambda.request_headers.get("x-auth"))
        self.assertTrue("authorization" in aws_lambda.request_headers)
        self.assertTrue("x-auth" in aws_lambda.request_headers)
        self.assertFalse("bearer" in aws_lambda.request_headers)

    def test_headers_v2(self):
        """Test header parsing for API Gateway v2."""
        aws_lambda = LambdaAPIGateway(self.dummy_event_v2, {})

        self.assertEqual("Bearer token", aws_lambda.request_headers.get("authorization"))
        self.assertEqual("application/json", aws_lambda.request_headers.get("content-type"))
        self.assertTrue("authorization" in aws_lambda.request_headers)
        self.assertTrue("content-type" in aws_lambda.request_headers)

    def test_body_plain_v1(self):
        """Test plain body parsing for API Gateway v1."""
        aws_lambda = LambdaAPIGateway(
            {**self.dummy_event_v1, **{"body": '{"hey": "sup"}', "isBase64Encoded": False}}, {}
        )

        self.assertEqual({"hey": "sup"}, aws_lambda.request_data)
        self.assertEqual('{"hey": "sup"}', aws_lambda.get_body())
        self.assertTrue(aws_lambda.has_body())

    def test_body_plain_v2(self):
        """Test plain body parsing for API Gateway v2."""
        aws_lambda = LambdaAPIGateway(
            {**self.dummy_event_v2, **{"body": '{"hello": "world"}', "isBase64Encoded": False}}, {}
        )

        self.assertEqual({"hello": "world"}, aws_lambda.request_data)
        self.assertEqual('{"hello": "world"}', aws_lambda.get_body())
        self.assertTrue(aws_lambda.has_body())

    def test_body_base64_v1(self):
        """Test base64 body parsing for API Gateway v1."""
        aws_lambda = LambdaAPIGateway(
            {**self.dummy_event_v1, **{"body": "eyJoZXkiOiAic3VwIn0=", "isBase64Encoded": True}}, {}
        )

        self.assertEqual({"hey": "sup"}, aws_lambda.request_data)
        self.assertEqual('{"hey": "sup"}', aws_lambda.get_body())
        self.assertTrue(aws_lambda.has_body())

    def test_body_base64_v2(self):
        """Test base64 body parsing for API Gateway v2."""
        aws_lambda = LambdaAPIGateway(
            {**self.dummy_event_v2, **{"body": "eyJoZWxsbyI6ICJ3b3JsZCJ9", "isBase64Encoded": True}}, {}
        )

        self.assertEqual({"hello": "world"}, aws_lambda.request_data)
        self.assertEqual('{"hello": "world"}', aws_lambda.get_body())
        self.assertTrue(aws_lambda.has_body())

    def test_path_v1(self):
        """Test path extraction for API Gateway v1."""
        aws_lambda = LambdaAPIGateway(self.dummy_event_v1, {})
        self.assertEqual("/test", aws_lambda.path)

    def test_path_v2(self):
        """Test path extraction for API Gateway v2."""
        aws_lambda = LambdaAPIGateway(self.dummy_event_v2, {})
        self.assertEqual("/test", aws_lambda.path)

    def test_query_parameters_v1(self):
        """Test query parameter parsing for API Gateway v1."""
        aws_lambda = LambdaAPIGateway(self.dummy_event_v1, {})
        expected = {"q": "hey", "bob": "sup", "tags": ["urgent", "important"]}
        self.assertEqual(expected, aws_lambda.query_parameters)

    def test_query_parameters_v2(self):
        """Test query parameter parsing for API Gateway v2."""
        aws_lambda = LambdaAPIGateway(self.dummy_event_v2, {})
        expected = {"q": "hello", "name": "world"}
        self.assertEqual(expected, aws_lambda.query_parameters)

    def test_path_parameters_v1(self):
        """Test path parameter parsing for API Gateway v1."""
        aws_lambda = LambdaAPIGateway(self.dummy_event_v1, {})
        self.assertEqual({"id": "123"}, aws_lambda.routing_data)

    def test_path_parameters_v2(self):
        """Test path parameter parsing for API Gateway v2."""
        aws_lambda = LambdaAPIGateway(self.dummy_event_v2, {})
        self.assertEqual({"id": "456"}, aws_lambda.routing_data)

    def test_resource_v1(self):
        """Test resource extraction for API Gateway v1."""
        aws_lambda = LambdaAPIGateway(self.dummy_event_v1, {})
        self.assertEqual("/test/{id}", aws_lambda.resource)

    def test_resource_v2(self):
        """Test resource extraction for API Gateway v2 (should be empty)."""
        aws_lambda = LambdaAPIGateway(self.dummy_event_v2, {})
        self.assertEqual("", aws_lambda.resource)

    def test_client_ip_v1(self):
        """Test client IP extraction for API Gateway v1."""
        aws_lambda = LambdaAPIGateway(self.dummy_event_v1, {})
        self.assertEqual("192.168.1.1", aws_lambda.get_client_ip())

    def test_client_ip_v2(self):
        """Test client IP extraction for API Gateway v2."""
        aws_lambda = LambdaAPIGateway(self.dummy_event_v2, {})
        self.assertEqual("192.168.1.2", aws_lambda.get_client_ip())

    def test_protocol_v1(self):
        """Test protocol for API Gateway v1 (defaults to https)."""
        aws_lambda = LambdaAPIGateway(self.dummy_event_v1, {})
        self.assertEqual("https", aws_lambda.get_protocol())

    def test_protocol_v2(self):
        """Test protocol for API Gateway v2."""
        aws_lambda = LambdaAPIGateway(self.dummy_event_v2, {})
        self.assertEqual("http", aws_lambda.get_protocol())

    def test_context_specifics_v1(self):
        """Test context specifics for API Gateway v1."""
        aws_lambda = LambdaAPIGateway(self.dummy_event_v1, {})
        context = aws_lambda.context_specifics()

        self.assertEqual("/test/{id}", context["resource"])
        self.assertEqual("/test", context["path"])
        self.assertEqual("prod", context["stage"])
        self.assertEqual("test-request-id", context["request_id"])
        self.assertEqual("test-api-id", context["api_id"])
        self.assertEqual("1.0", context["api_version"])

    def test_context_specifics_v2(self):
        """Test context specifics for API Gateway v2."""
        aws_lambda = LambdaAPIGateway(self.dummy_event_v2, {})
        context = aws_lambda.context_specifics()

        self.assertEqual("", context["resource"])
        self.assertEqual("/test", context["path"])
        self.assertEqual("dev", context["stage"])
        self.assertEqual("test-request-id-v2", context["request_id"])
        self.assertEqual("test-api-id-v2", context["api_id"])
        self.assertEqual("api.example.com", context["domain_name"])
        self.assertEqual("HTTP/1.1", context["protocol"])
        self.assertEqual("test-user-agent", context["user_agent"])
        self.assertEqual("2.0", context["api_version"])

    def test_version_detection_v1(self):
        """Test version detection for API Gateway v1."""
        aws_lambda = LambdaAPIGateway(self.dummy_event_v1, {})
        self.assertEqual("1.0", aws_lambda._detect_version(self.dummy_event_v1))

    def test_version_detection_v2(self):
        """Test version detection for API Gateway v2."""
        aws_lambda = LambdaAPIGateway(self.dummy_event_v2, {})
        self.assertEqual("2.0", aws_lambda._detect_version(self.dummy_event_v2))

    def test_request_method_v1(self):
        """Test request method for API Gateway v1."""
        aws_lambda = LambdaAPIGateway(self.dummy_event_v1, {})
        self.assertEqual("GET", aws_lambda.request_method)

    def test_request_method_v2(self):
        """Test request method for API Gateway v2."""
        aws_lambda = LambdaAPIGateway(self.dummy_event_v2, {})
        self.assertEqual("GET", aws_lambda.request_method)
