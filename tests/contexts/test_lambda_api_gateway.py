from __future__ import annotations

import json
import unittest

import clearskies
from clearskies import columns, validators

from clearskies_aws.contexts.lambda_api_gateway import LambdaApiGateway

from .my_awesome_model import MyAwesomeModel


class LambdaApiGatewayTest(unittest.TestCase):
    def setUp(self):
        clearskies.backends.MemoryBackend.clear_table_cache()
        self.application = LambdaApiGateway(
            clearskies.endpoints.Create(
                MyAwesomeModel,
                readable_column_names=["id", "name", "email", "created_at"],
                writeable_column_names=["name", "email"],
                url="/model",
            )
        )

    def test_create_v1(self):
        response = self.application(
            {
                "resource": "/",
                "path": "/model",
                "httpMethod": "POST",
                "headers": {},
                "queryStringParameters": None,
                "pathParameters": [],
                "stageVariables": [],
                "requestContext": {},
                "body": json.dumps(
                    {
                        "name": "Bob",
                        "email": "bob@example.com",
                    }
                ),
                "isBase64Encoded": False,
            },
            {},
        )

        response_data = json.loads(response["body"])["data"]
        assert response["statusCode"] == 200
        assert response_data["name"] == "Bob"
        assert response_data["email"] == "bob@example.com"

    def test_create_v2(self):
        response = self.application(
            {
                "version": "2.0",
                "routeKey": "$default",
                "rawPath": "/model",
                "rawQueryString": "parameter1=value1&parameter1=value2&parameter2=value",
                "cookies": [],
                "headers": {},
                "queryStringParameters": {},
                "requestContext": {
                    "accountId": "123456789012",
                    "apiId": "api-id",
                    "authentication": {"clientCert": {}},
                    "authorizer": {},
                    "domainName": "id.execute-api.us-east-1.amazonaws.com",
                    "domainPrefix": "id",
                    "http": {
                        "method": "POST",
                        "path": "/model",
                        "protocol": "HTTP/1.1",
                        "sourceIp": "IP",
                        "userAgent": "agent",
                    },
                    "requestId": "id",
                    "routeKey": "$default",
                    "stage": "$default",
                    "time": "12/Mar/2020:19:03:58 +0000",
                    "timeEpoch": 1583348638390,
                },
                "body": json.dumps(
                    {
                        "name": "Bob",
                        "email": "bob@example.com",
                    }
                ),
                "pathParameters": {},
                "isBase64Encoded": False,
                "stageVariables": {},
            },
            {},
        )
        response_data = json.loads(response["body"])["data"]
        assert response["statusCode"] == 200
        assert response_data["name"] == "Bob"
        assert response_data["email"] == "bob@example.com"

    def test_404(self):
        response = self.application(
            {
                "resource": "/",
                "path": "/wrong-url",
                "httpMethod": "POST",
                "headers": {},
                "queryStringParameters": None,
                "pathParameters": [],
                "stageVariables": [],
                "requestContext": {},
                "body": json.dumps(
                    {
                        "name": "Bob",
                        "email": "bob@example.com",
                    }
                ),
                "isBase64Encoded": False,
            },
            {},
        )
        assert response["statusCode"] == 404

        response = self.application(
            {
                "httpMethod": "GET",
                "path": "/model",
                "queryStringParameters": {},
                "headers": {},
                "body": json.dumps(
                    {
                        "name": "Bob",
                        "email": "bob@example.com",
                    }
                ),
                "isBase64Encoded": False,
            },
            {},
        )
        assert response["statusCode"] == 404
