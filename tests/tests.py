import unittest
from unittest.mock import patch, Mock
from unittest.mock import MagicMock

from src.boardsonfire_client.client import BoardsOnFireClient, Response
from src.boardsonfire_client.endpoints import (
    Organizations,
    Users,
    Entities,
    DataSources,
    ValidateEntity,
    ValidateDataSource,
)
from src.boardsonfire_client.exceptions import (
    RateLimitException,
    NotFoundException,
    RestClientException,
    ValidationError,
)


class TestBoardsOnFireClient(unittest.TestCase):

    def setUp(self):
        self.client = BoardsOnFireClient("example.com", "api_key")

    @patch("requests.request")
    def test_send_request_success(self, mock_request):
        mock_response = Mock()
        mock_response.ok = True
        mock_response.json.return_value = {"data": "test"}
        mock_response.status_code = 200
        mock_request.return_value = mock_response

        response = self.client._send_request("GET", "endpoint")
        self.assertEqual(response.data, {"data": "test"})

    @patch("requests.request")
    def test_send_request_rate_limit(self, mock_request):
        mock_response = Mock()
        mock_response.ok = False
        mock_response.status_code = 429
        mock_request.return_value = mock_response

        with self.assertRaises(RateLimitException):
            self.client._send_request("GET", "endpoint")

    @patch("requests.request")
    def test_send_request_not_found(self, mock_request):
        mock_response = Mock()
        mock_response.ok = False
        mock_response.status_code = 404
        mock_request.return_value = mock_response

        with self.assertRaises(NotFoundException):
            self.client._send_request("GET", "endpoint")

    @patch("requests.request")
    def test_send_request_bad_response(self, mock_request):
        mock_response = Mock()
        mock_response.ok = False
        mock_response.status_code = 400
        mock_request.return_value = mock_response

        with self.assertRaises(RestClientException):
            self.client._send_request("GET", "endpoint")

    def test_get_request(self):
        self.client._send_request = MagicMock(return_value=Response(200, {}))

        response = self.client._get("endpoint")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers, {})
        self.assertIsNone(response.data)

    def test_post_request(self):
        self.client._send_request = MagicMock(return_value=Response(200, {}))

        response = self.client._post("endpoint", data={"key": "value"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers, {})
        self.assertIsNone(response.data)

    def test_delete_request(self):
        self.client._send_request = MagicMock(return_value=Response(200, {}))

        response = self.client._delete("endpoint")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers, {})

    def test_patch_request(self):
        self.client._send_request = MagicMock(return_value=Response(200, {}))

        response = self.client._patch("endpoint", data={"key": "value"})

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers, {})
        self.assertIsNone(response.data)

    def test_rate_limit_exceeded(self):
        self.client._send_request = MagicMock(return_value=Response(429, {}))

        response = self.client._get("endpoint")

        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.headers, {})


class TestOrganizations(unittest.TestCase):
    def setUp(self):
        self.rest_client = MagicMock()
        self.organizations = Organizations(self.rest_client)

    def test_list(self):
        self.rest_client._get.return_value = MagicMock(
            data=[{"id": "1", "name": "Org1"}, {"id": "2", "name": "Org2"}]
        )
        result = self.organizations.list()
        self.rest_client._get.assert_called_with(
            "organizations",
            params={"page_size": 50, "page": 1, "order": None, "direction": "ASC"},
        )
        self.assertEqual(
            result, [{"id": "1", "name": "Org1"}, {"id": "2", "name": "Org2"}]
        )

    def test_list_all(self):
        self.rest_client._get.return_value = MagicMock(
            data=[{"id": "1", "name": "Org1"}, {"id": "2", "name": "Org2"}]
        )
        result = list(self.organizations.list_all(limit=100))
        self.rest_client._get.assert_called_with(
            "organizations",
            params={"page_size": 100, "page": 1, "order": None, "direction": "ASC"},
        )
        self.assertEqual(
            result, [{"id": "1", "name": "Org1"}, {"id": "2", "name": "Org2"}]
        )

    def test_get(self):
        self.rest_client._get.return_value = MagicMock(data={"id": "1", "name": "Org1"})
        result = self.organizations.get("1")
        self.rest_client._get.assert_called_with("organizations/1")
        self.assertEqual(result, {"id": "1", "name": "Org1"})


class TestUsers(unittest.TestCase):
    def setUp(self):
        self.rest_client = MagicMock()
        self.users = Users(self.rest_client)

    def test_list(self):
        self.rest_client._get.return_value.data = [{"id": "1", "name": "Test User"}]
        response = self.users.list()
        self.assertEqual(response, [{"id": "1", "name": "Test User"}])
        self.rest_client._get.assert_called_once_with(
            "users",
            params={
                "page_size": 100,
                "page": 1,
                "order": None,
                "direction": "ASC",
            },
        )

    def test_list_all(self):
        self.rest_client._get.return_value.data = [{"id": "1", "name": "Test User"}]
        response = list(self.users.list_all())
        self.assertEqual(response, [{"id": "1", "name": "Test User"}])
        self.rest_client._get.assert_called_once_with(
            "users",
            params={
                "page_size": 100,
                "page": 1,
                "order": None,
                "direction": "ASC",
            },
        )

    def test_get(self):
        self.rest_client._get.return_value.data = {"id": "1", "name": "Test User"}
        response = self.users.get("1")
        self.assertEqual(response, {"id": "1", "name": "Test User"})
        self.rest_client._get.assert_called_once_with("users/1")


class TestEntities(unittest.TestCase):
    def setUp(self):
        self.rest_client = MagicMock()
        self.entities = Entities(self.rest_client)
        self.entity_name = "test_entity"

    def test_list(self):
        self.rest_client._post.return_value.data = [{"id": "1", "name": "Test Entity"}]
        response = self.entities.list(entity_name=self.entity_name)
        self.assertEqual(response, [{"id": "1", "name": "Test Entity"}])
        self.rest_client._post.assert_called_once_with(
            f"entities/{self.entity_name}/entityobjects/list",
            params={
                "page_size": 100,
                "page": 1,
                "order": None,
                "group": None,
                "filter": None,
                "target_organization_ids": "",
            },
        )

    def test_list_all(self):
        self.rest_client._post.return_value.data = [{"id": "1", "name": "Test Entity"}]
        response = list(self.entities.list_all(entity_name=self.entity_name))
        self.assertEqual(response, [{"id": "1", "name": "Test Entity"}])
        self.rest_client._post.assert_called_once_with(
            f"entities/{self.entity_name}/entityobjects/list",
            params={
                "page_size": 100,
                "page": 1,
                "order": None,
                "group": None,
                "filter": None,
                "target_organization_ids": "",
            },
        )

    def test_get(self):
        self.rest_client._get.return_value.data = {"id": "1", "name": "Test Entity"}
        response = self.entities.get(entity_name=self.entity_name, id="1")
        self.assertEqual(response, {"id": "1", "name": "Test Entity"})
        self.rest_client._get.assert_called_once_with(
            f"entities/{self.entity_name}/entityobjects/1"
        )

    def test_create(self):
        data = {
            "name": "Example",
            "description": "This is an example entity",
            "value": 100,
            "organization_id": "123456",
        }
        response_data = {
            "id": "123456",
            "name": "Example",
            "description": "This is an example entity",
            "value": 100,
        }
        self.rest_client._post.return_value.data = response_data
        result = self.entities.create(self.entity_name, data)

        self.assertEqual(result, response_data)

        self.rest_client._post.assert_called_once_with(
            f"entities/{self.entity_name}/entityobjects", data=data
        )

    def test_create_without_org_id(self):
        data = {
            "name": "Example",
            "description": "This is an example entity",
            "value": 100,
        }
        self.assertRaises(ValidationError, self.entities.create, self.entity_name, data)

    def test_upsert_without_org_id(self):
        data = [
            {
                "name": "Example",
                "description": "This is an example entity",
                "value": 100,
            }
        ]
        self.assertRaises(ValidationError, self.entities.upsert, self.entity_name, data)


class TestDataSources(unittest.TestCase):
    def setUp(self):
        self.rest_client = MagicMock()
        self.datasources = DataSources(self.rest_client)
        self.datasource_name = "test_datasource"

    def test_list(self):
        self.rest_client._post.return_value.data = [{"id": "1", "name": "Test"}]
        response = self.datasources.list(datasource_name=self.datasource_name)
        self.assertEqual(response, [{"id": "1", "name": "Test"}])
        self.rest_client._post.assert_called_once_with(
            f"datasources/{self.datasource_name}/dataobjects/list",
            data={
                "page_size": 100,
                "page": 1,
                "order": None,
                "group": None,
                "filter": None,
                "target_organization_ids": "",
            },
        )

    def test_list_all(self):
        self.rest_client._post.return_value.data = [{"id": "1", "name": "Test"}]
        response = list(self.datasources.list_all(datasource_name=self.datasource_name))
        self.assertEqual(response, [{"id": "1", "name": "Test"}])
        self.rest_client._post.assert_called_once_with(
            f"datasources/{self.datasource_name}/dataobjects/list",
            data={
                "page_size": 100,
                "page": 1,
                "order": None,
                "group": None,
                "filter": None,
                "target_organization_ids": "",
            },
        )

    def test_get(self):
        self.rest_client._get.return_value.data = {"id": "1", "name": "Test"}
        response = self.datasources.get(datasource_name=self.datasource_name, id="1")
        self.assertEqual(response, {"id": "1", "name": "Test"})
        self.rest_client._get.assert_called_once_with(
            f"datasources/{self.datasource_name}/dataobjects/1"
        )

    def test_create(self):
        data = {
            "name": "Example",
            "description": "This is an example dataspurce",
            "value": 100,
            "organization_id": "123456",
            "timestamp": "2022-01-01T00:00:00Z",
        }
        response_data = {
            "id": "123456",
            "name": "Example",
            "description": "This is an example dataspurce",
            "value": 100,
        }
        self.rest_client._post.return_value.data = response_data
        result = self.datasources.create(self.datasource_name, data)

        self.assertEqual(result, response_data)

        self.rest_client._post.assert_called_once_with(
            f"datasources/{self.datasource_name}/dataobjects", data=data
        )

    def test_create_without_org_id(self):
        data = {
            "name": "Example",
            "description": "This is an example datasource",
            "value": 100,
        }
        self.assertRaises(
            ValidationError, self.datasources.create, self.datasource_name, data
        )

    def test_upsert_without_org_id(self):
        data = [
            {
                "name": "Example",
                "description": "This is an example entity",
                "value": 100,
            }
        ]
        self.assertRaises(
            ValidationError, self.datasources.upsert, self.datasource_name, data
        )


class TestValidateEntity(unittest.TestCase):
    def test_create(self):
        data = {}
        with self.assertRaises(ValidationError):
            ValidateEntity.create(data)

        data = {"organization_id": "123"}
        try:
            ValidateEntity.create(data)
        except ValidationError:
            self.fail("ValidateEntity.create() raised ValidationError unexpectedly!")

    def test_upsert(self):

        data = [{}]
        with self.assertRaises(ValidationError):
            ValidateEntity.upsert(data)

        data = [{"organization_id": "123"}, {"organization_id": "456"}]
        try:
            ValidateEntity.upsert(data)
        except ValidationError:
            self.fail("ValidateEntity.upsert() raised ValidationError unexpectedly!")


class TestValidateDataSource(unittest.TestCase):
    def test_create(self):
        valid_data = {"organization_id": "123", "timestamp": "2022-01-01T00:00:00Z"}
        invalid_data = {"organization_id": "123"}

        try:
            ValidateDataSource.create(valid_data)
        except ValidationError:
            self.fail("ValidateDataSource.create raised ValidationError unexpectedly!")

        with self.assertRaises(ValidationError):
            ValidateDataSource.create(invalid_data)

    def test_upsert(self):
        valid_data = [{"organization_id": "123", "timestamp": "2022-01-01T00:00:00Z"}]
        invalid_data = [{"organization_id": "123"}]

        try:
            ValidateDataSource.upsert(valid_data)
        except ValidationError:
            self.fail("ValidateDataSource.upsert raised ValidationError unexpectedly!")

        with self.assertRaises(ValidationError):
            ValidateDataSource.upsert(invalid_data)


if __name__ == "__main__":
    unittest.main()
