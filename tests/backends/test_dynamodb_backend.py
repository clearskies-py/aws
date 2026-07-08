from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, call

import clearskies

from clearskies_aws.backends.dynamodb_backend import DynamodbBackend
from clearskies_aws.models import DynamodbModel


class User(DynamodbModel):
    id_column_name = "id"
    backend = DynamodbBackend(client_injection_name="dynamodb_client")
    id = clearskies.columns.Uuid()
    name = clearskies.columns.String()
    age = clearskies.columns.Integer()
    category = clearskies.columns.Select(["Auto", "Toy"])


class DynamodbBackendTest(unittest.TestCase):
    def setUp(self):
        self.mock_dynamodb_client = SimpleNamespace(execute_statement=MagicMock())
        self.mock_dynamodb_client.execute_statement.side_effect = [
            {
                "ConsumedCapacity": {},
                "NextToken": "asdf",
                "Items": [],
            },
            {
                "ConsumedCapacity": {},
                "NextToken": "asdf",
                "Items": [
                    {
                        "id": {"S": "1-2-3-4"},
                        "name": {"S": "Bob"},
                        "age": {"N": "15"},
                    }
                ],
            },
        ]
        self.environment = SimpleNamespace(get=MagicMock(return_value="us-east-1"))
        self.uuid = MagicMock()
        self.uuid.uuid4 = MagicMock(return_value="1-2-3-4")
        User.backend._cursor = None

    def test_create_records(self):
        def create_records(users):
            users.create(
                {
                    "name": "Bob",
                    "age": 20,
                    "category": "Auto",
                }
            )

        context = clearskies.contexts.Context(
            create_records,
            classes=[User],
            bindings={
                "environment": self.environment,
                "dynamodb_client": self.mock_dynamodb_client,
                "uuid": self.uuid,
            },
        )
        (status_code, response_data, response_headers) = context()
        assert 2 == self.mock_dynamodb_client.execute_statement.call_count
        assert (
            call(
                Statement="INSERT INTO \"users\" VALUE {'name': ?, 'age': ?, 'category': ?, 'id': ?}",
                Parameters=[{"S": "Bob"}, {"N": "20"}, {"S": "Auto"}, {"S": "1-2-3-4"}],
                ReturnConsumedCapacity="INDEXES",
            )
            == self.mock_dynamodb_client.execute_statement.call_args_list[0]
        )
        assert (
            call(
                Statement='SELECT * FROM "users" WHERE id=?',
                Parameters=[{"S": "1-2-3-4"}],
                ReturnConsumedCapacity="INDEXES",
            )
            == self.mock_dynamodb_client.execute_statement.call_args_list[1]
        )

    def test_update_records(self):
        def update_records(users):
            user = users.model({"id": "1-2-3-4", "name": "Bob", "category": "Auto"})
            user.save(
                {
                    "name": "Alice",
                    "age": 20,
                    "category": "Toy",
                }
            )

        context = clearskies.contexts.Context(
            update_records,
            classes=[User],
            bindings={
                "environment": self.environment,
                "dynamodb_client": self.mock_dynamodb_client,
                "uuid": self.uuid,
            },
        )
        (status_code, response_data, response_headers) = context()
        assert 2 == self.mock_dynamodb_client.execute_statement.call_count
        assert (
            call(
                Statement='UPDATE "users" SET "name"=?, "age"=?, "category"=? WHERE "id"=? AND "name"=? AND "category"=?',
                Parameters=[{"S": "Alice"}, {"N": "20"}, {"S": "Toy"}, {"S": "1-2-3-4"}, {"S": "Bob"}, {"S": "Auto"}],
                ReturnConsumedCapacity="INDEXES",
            )
            == self.mock_dynamodb_client.execute_statement.call_args_list[0]
        )
        assert (
            call(
                Statement='SELECT * FROM "users" WHERE id=?',
                Parameters=[{"S": "1-2-3-4"}],
                ReturnConsumedCapacity="INDEXES",
            )
            == self.mock_dynamodb_client.execute_statement.call_args_list[1]
        )

    def test_delete_records(self):
        def delete_records(users):
            user = users.model({"id": "1-2-3-4", "name": "Bob", "category": "Auto"})
            user.delete()

        context = clearskies.contexts.Context(
            delete_records,
            classes=[User],
            bindings={
                "environment": self.environment,
                "dynamodb_client": self.mock_dynamodb_client,
                "uuid": self.uuid,
            },
        )
        (status_code, response_data, response_headers) = context()
        assert 1 == self.mock_dynamodb_client.execute_statement.call_count
        assert (
            call(
                Statement='DELETE FROM "users" WHERE "id"=? AND "name"=? AND "category"=?',
                Parameters=[{"S": "1-2-3-4"}, {"S": "Bob"}, {"S": "Auto"}],
                ReturnConsumedCapacity="INDEXES",
            )
            == self.mock_dynamodb_client.execute_statement.call_args_list[0]
        )

    def test_list_records_scan(self):
        def list_records_scan(users):
            return [user for user in users.where("id=1-2-3-4").where("category=Auto")]

        context = clearskies.contexts.Context(
            list_records_scan,
            classes=[User],
            bindings={
                "environment": self.environment,
                "dynamodb_client": self.mock_dynamodb_client,
            },
        )
        (status_code, response_data, response_headers) = context()
        assert 1 == self.mock_dynamodb_client.execute_statement.call_count
        assert (
            call(
                Statement='SELECT * FROM "users" WHERE id=? AND category=?',
                Parameters=[{"S": "1-2-3-4"}, {"S": "Auto"}],
                ReturnConsumedCapacity="INDEXES",
            )
            == self.mock_dynamodb_client.execute_statement.call_args_list[0]
        )

    def test_list_records_query(self):
        def list_records_query(users):
            query = users.where("category=toy").query_with_index("category-age").sort_by("age", "desc")
            fetched = [user for user in query]
            return query.next_page_data()

        context = clearskies.contexts.Context(
            list_records_query,
            classes=[User],
            bindings={
                "environment": self.environment,
                "dynamodb_client": self.mock_dynamodb_client,
            },
        )
        (status_code, response_data, response_headers) = context()
        assert 1 == self.mock_dynamodb_client.execute_statement.call_count
        assert (
            call(
                Statement='SELECT * FROM "users"."category-age" WHERE category=? ORDER BY age DESC',
                Parameters=[{"S": "toy"}],
                ReturnConsumedCapacity="INDEXES",
            )
            == self.mock_dynamodb_client.execute_statement.call_args_list[0]
        )
        assert response_data == {"next_token": "asdf"}
