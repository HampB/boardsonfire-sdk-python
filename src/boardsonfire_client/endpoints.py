from typing import List, Dict, Iterator

from .client import BoardsOnFireClient as RestClient
from .exceptions import ValidationError

MAX_PAGE_SIZE = 500


class ValidateEntity:
    @staticmethod
    def create(data: Dict) -> None:
        """
        Validate the data for creating an entity object.

        Args:
            data (Dict): The data to be validated.

        Raises:
            ValidationError: If the required fields are missing.
        """
        if not {"organization_id"} <= set(data):
            raise ValidationError(
                "organization_id is required to create an entity object"
            )

    @staticmethod
    def upsert(data: List[Dict]) -> None:
        """
        Validate the data for upserting entity objects.

        Args:
            data (List[Dict]): The data to be validated.

        Raises:
            ValidationError: If the required fields are missing.
        """
        if not isinstance(data, list):
            raise ValidationError("Data must be a list of dictionaries")

        if not all({"organization_id"} <= set(row) for row in data):
            raise ValidationError(
                "organization_id is required to create an entity object"
            )


class ValidateDataSource:
    @staticmethod
    def create(data: Dict) -> None:
        """
        Validate the data for creating a datasource object.

        Args:
            data (Dict): The data to be validated.

        Raises:
            ValidationError: If the required fields are missing.
        """
        if not {"organization_id", "timestamp"} <= set(data):
            raise ValidationError(
                "organization_id and timestamp is required to create a datasource object"
            )

    @staticmethod
    def upsert(data: List[Dict]) -> None:
        """
        Validate the data for upserting datasource objects.

        Args:
            data (List[Dict]): The data to be validated.

        Raises:
            ValidationError: If the required fields are missing.
        """
        if not isinstance(data, list):
            raise ValidationError("Data must be a list of dictionaries")

        if not all({"organization_id", "timestamp"} <= set(row) for row in data):
            raise ValidationError(
                "organization_id and timestamp is required to create a datasource object"
            )


class Organizations:
    """
    Represents a collection of methods for interacting with organizations.
    """

    def __init__(self, rest_client: RestClient):
        self.rest_client = rest_client

    def list(
        self,
        page_size: int = 50,
        page: int = 1,
        order_by: str = None,
        direction: str = "ASC",
    ) -> List[Dict]:
        """
        List organizations.

        Args:
            page_size (int, optional): The number of organizations to retrieve per page. Defaults to 50.
            page (int, optional): The page number to retrieve. Defaults to 1.
            order_by (str, optional): The field to order the organizations by. Defaults to None.
            direction (str, optional): The direction of the ordering (ASC or DESC). Defaults to ASC.

        Returns:
            List[Dict]: The list of organizations.
        """
        if page_size > MAX_PAGE_SIZE:
            self.rest_client._logger.warning(
                (
                    f"The maximum page size is {MAX_PAGE_SIZE}. Response is truncated. \n"
                    f"Please consider using the list_all method for large datasets."
                )
            )
            page_size = MAX_PAGE_SIZE

        params = {
            "page_size": page_size,
            "page": page,
            "order": order_by,
            "direction": direction,
        }
        response = self.rest_client._get("organizations", params=params)
        return response.data

    def list_all(
        self, limit: int = None, order_by: str = None, direction: str = "ASC"
    ) -> Iterator[Dict]:
        """
        List all organizations.

        Args:
            limit (int, optional): The maximum number of organizations to retrieve. Defaults to None.
            order_by (str, optional): The field to order the organizations by. Defaults to None.
            direction (str, optional): The direction of the ordering (ASC or DESC). Defaults to ASC.

        Yields:
            Iterator[Dict]: An iterator over the organizations.
        """
        params = {
            "page_size": min(100, limit if limit else 100),
            "page": 1,
            "order": order_by,
            "direction": direction,
        }
        yield_count = 0
        trigger = True
        while trigger:
            response = self.rest_client._get("organizations", params=params)

            for record in response.data:
                yield_count += 1
                yield record

                if limit and yield_count >= limit:
                    trigger = False
                    break

            if not response.data or len(response.data) < params["page_size"]:
                break

            params["page"] += 1

    def get(self, id: str) -> Dict:
        """
        Get an organization by ID.

        Args:
            id (str): The ID of the organization.

        Returns:
            Dict: The organization data.
        """
        response = self.rest_client._get(f"organizations/{id}")
        return response.data


class Users:
    """
    Represents a collection of methods for interacting with users.
    """

    def __init__(self, rest_client: RestClient):
        self.rest_client = rest_client

    def list(
        self,
        page_size: int = 100,
        page: int = 1,
        order_by: str = None,
        direction: str = "ASC",
    ) -> List[Dict]:
        """
        List users.

        Args:
            page_size (int, optional): The number of users to retrieve per page. Defaults to 100.
            page (int, optional): The page number to retrieve. Defaults to 1.
            order_by (str, optional): The field to order the users by. Defaults to None.
            direction (str, optional): The direction of the ordering (ASC or DESC). Defaults to ASC.

        Returns:
            List[Dict]: The list of users.
        """
        if page_size > MAX_PAGE_SIZE:
            self.rest_client._logger.warning(
                (
                    f"The maximum page size is {MAX_PAGE_SIZE}. Response is truncated. \n"
                    f"Please consider using the list_all method for large datasets."
                )
            )
            page_size = MAX_PAGE_SIZE

        params = {
            "page_size": page_size,
            "page": page,
            "order": order_by,
            "direction": direction,
        }
        response = self.rest_client._get("users", params=params)
        return response.data

    def list_all(
        self, limit: int = None, order_by: str = None, direction: str = "ASC"
    ) -> Iterator[Dict]:
        """
        List all users.

        Args:
            limit (int, optional): The maximum number of users to retrieve. Defaults to None.
            order_by (str, optional): The field to order the users by. Defaults to None.
            direction (str, optional): The direction of the ordering (ASC or DESC). Defaults to ASC.

        Yields:
            Iterator[Dict]: An iterator over the users.
        """
        params = {
            "page_size": min(100, limit if limit else 100),
            "page": 1,
            "order": order_by,
            "direction": direction,
        }
        yield_count = 0
        trigger = True
        while trigger:
            response = self.rest_client._get("users", params=params)

            for record in response.data:
                yield_count += 1
                yield record

                if limit and yield_count >= limit:
                    trigger = False
                    break

            if not response.data or len(response.data) < params["page_size"]:
                break

            params["page"] += 1

    def get(self, id: str) -> Dict:
        """
        Get a user by ID.

        Args:
            id (str): The ID of the user.

        Returns:
            Dict: The user data.
        """
        response = self.rest_client._get(f"users/{id}")
        return response.data


class Entities:
    """
    Represents a collection of methods for interacting with entites.
    """

    def __init__(self, rest_client: RestClient):
        self.rest_client = rest_client

    def list(
        self,
        entity_name: str,
        organizations: List[str] = [],
        page_size: int = 100,
        page: int = 1,
        order: str = None,
        group: str = None,
        filter: str = None,
    ) -> List[Dict]:
        """
        List entity objects.

        Args:
            entity_name (str): The name of the entity.
            organizations (List[str], optional): The list of organization IDs to filter by. Defaults to [].
            page_size (int, optional): The number of entity objects to retrieve per page. Defaults to 100.
            page (int, optional): The page number to retrieve. Defaults to 1.
            order (str, optional): The field to order the entity objects by. Defaults to None.
            group (str, optional): Filter the data using group. Defaults to None.
            filter (str, optional): The filter to apply to the entity objects. Defaults to None.

        Returns:
            List[Dict]: The list of entity objects.
        """
        if page_size > MAX_PAGE_SIZE:
            self.rest_client._logger.warning(
                (
                    f"The maximum page size is {MAX_PAGE_SIZE}. Response is truncated. \n"
                    f"Please consider using the list_all method for large datasets."
                )
            )
            page_size = MAX_PAGE_SIZE

        params = {
            "page_size": page_size,
            "page": page,
            "order": order,
            "group": group,
            "filter": filter,
            "target_organization_ids": ",".join(organizations),
        }
        response = self.rest_client._post(
            f"entities/{entity_name}/entityobjects/list", params=params
        )
        return response.data

    def list_all(
        self,
        entity_name: str,
        limit: int = None,
        organizations: List[str] = [],
        order: str = None,
        group: str = None,
        filter: str = None,
    ) -> Iterator[Dict]:
        """
        List all entity objects.

        Args:
            entity_name (str): The name of the entity.
            limit (int, optional): The maximum number of entity objects to retrieve. Defaults to None.
            organizations (List[str], optional): The list of organization IDs to filter by. Defaults to [].
            order (str, optional): The field to order the entity objects by. Defaults to None.
            group (str, optional): Filter the data using group. Defaults to None.
            filter (str, optional): The filter to apply to the entity objects. Defaults to None.

        Yields:
            Iterator[Dict]: An iterator over the entity objects.
        """
        params = {
            "page_size": min(100, limit if limit else 100),
            "page": 1,
            "order": order,
            "group": group,
            "filter": filter,
            "target_organization_ids": ",".join(organizations),
        }
        yield_count = 0
        over_limit = False
        while not over_limit:
            response = self.rest_client._post(
                f"entities/{entity_name}/entityobjects/list", params=params
            )

            for record in response.data:
                yield_count += 1
                yield record

                if limit and yield_count >= limit:
                    over_limit = True
                    break

            if not response.data or len(response.data) < params["page_size"]:
                break

            params["page"] += 1

    def get(self, entity_name: str, id: str) -> Dict:
        """
        Get an entity object by ID.

        Args:
            entity_name (str): The name of the entity.
            id (str): The ID of the entity object.

        Returns:
            Dict: The entity object data.
        """
        response = self.rest_client._get(f"entities/{entity_name}/entityobjects/{id}")
        return response.data

    def update(self, entity_name: str, id: str, data: Dict) -> Dict:
        """
        Update an entity object.

        Args:
            entity_name (str): The name of the entity.
            id (str): The ID of the entity object.
            data (Dict): The updated data for the entity object.

        Returns:
            Dict: The updated entity object data.
        """
        response = self.rest_client._patch(
            f"entities/{entity_name}/entityobjects/{id}", data=data
        )
        return response.data

    def create(self, entity_name: str, data: Dict) -> Dict:
        """
        Create an entity object.

        Args:
            entity_name (str): The name of the entity.
            data (Dict): The data for the entity object.

        Returns:
            Dict: The created entity object data.
        """
        ValidateEntity.create(data)

        response = self.rest_client._post(
            f"entities/{entity_name}/entityobjects", data=data
        )
        return response.data

    def upsert(
        self, entity_name: str, data: List[Dict], truncate: bool = False
    ) -> List[str]:
        """
        Upsert entity objects.

        Args:
            entity_name (str): The name of the entity.
            data (List[Dict]): The data for the entity objects.
            truncate (bool, optional): Whether to delete other entity objects not included in the data. Defaults to False.

        Returns:
            List[str]: The IDs of the upserted entity objects.
        """
        ValidateEntity.upsert(data)

        body = {"entity_objects": data, "delete_others": truncate}
        response = self.rest_client._post(
            f"entities/{entity_name}/entityobjects/import", data=body
        )
        return response.data

    def delete(self, entity_name: str, id: str) -> None:
        """
        Delete an entity object.

        Args:
            entity_name (str): The name of the entity.
            id (str): The ID of the entity object.
        """
        self.rest_client._delete(f"entities/{entity_name}/entityobjects/{id}")


class DataSources:
    """
    Represents a collection of methods for interacting with data sources.
    """

    def __init__(self, rest_client: RestClient):
        self.rest_client = rest_client

    def list(
        self,
        datasource_name: str,
        organizations: List[str] = [],
        page_size: int = 100,
        page: int = 1,
        order: str = None,
        group: str = None,
        filter: str = None,
    ) -> List[Dict]:
        """
        List data objects from a datasource.

        Args:
            datasource_name (str): The name of the datasource.
            organizations (List[str], optional): The list of organization IDs to filter by. Defaults to [].
            page_size (int, optional): The number of data objects to retrieve per page. Defaults to 100.
            page (int, optional): The page number to retrieve. Defaults to 1.
            order (str, optional): The field to order the data objects by  i.e. (columnName asc). Defaults to None.
            group (str, optional): Filter the data using group. Defaults to None.
            filter (str, optional): The filter to apply to the data objects. Defaults to None.

        Returns:
            List[Dict]: The list of data objects.
        """
        if page_size > MAX_PAGE_SIZE:
            self.rest_client._logger.warning(
                (
                    f"The maximum page size is {MAX_PAGE_SIZE}. Response is truncated. \n"
                    f"Please consider using the list_all method for large datasets."
                )
            )
            page_size = MAX_PAGE_SIZE

        body = {
            "page_size": page_size,
            "page": page,
            "order": order,
            "group": group,
            "filter": filter,
            "target_organization_ids": ",".join(organizations),
        }
        response = self.rest_client._post(
            f"datasources/{datasource_name}/dataobjects/list", data=body
        )
        return response.data

    def list_all(
        self,
        datasource_name: str,
        limit: int = None,
        organizations: List[str] = [],
        order: str = None,
        group: str = None,
        filter: str = None,
    ) -> Iterator[Dict]:
        """
        List all data objects from a datasource.

        Args:
            datasource_name (str): The name of the datasource.
            limit (int, optional): The maximum number of data objects to retrieve. Defaults to None.
            organizations (List[str], optional): The list of organization IDs to filter by. Defaults to [].
            order (str, optional): The field to order the data objects by  i.e. (columnName asc). Defaults to None.
            group (str, optional): Filter the data using group. Defaults to None.
            filter (str, optional): The filter to apply to the data objects i.e. (columnName asc). Defaults to None.

        Yields:
            Iterator[Dict]: An iterator over the data objects.
        """
        body = {
            "page_size": min(100, limit if limit else 100),
            "page": 1,
            "order": order,
            "group": group,
            "filter": filter,
            "target_organization_ids": ",".join(organizations),
        }
        yield_count = 0
        over_limit = False
        while not over_limit:
            response = self.rest_client._post(
                f"datasources/{datasource_name}/dataobjects/list", data=body
            )

            for record in response.data:
                yield_count += 1
                yield record

                if limit and yield_count >= limit:
                    over_limit = True
                    break

            if not response.data or len(response.data) < body["page_size"]:
                break

            body["page"] += 1

    def get(self, datasource_name: str, id: str) -> Dict:
        """
        Get a specific data object from a datasource.

        Args:
            datasource_name (str): The name of the datasource.
            id (str): The ID of the data object.

        Returns:
            Dict: The data object.
        """
        response = self.rest_client._get(
            f"datasources/{datasource_name}/dataobjects/{id}"
        )
        return response.data

    def update(self, datasource_name: str, id: str, data: Dict) -> Dict:
        """
        Update a specific data object in a datasource.

        Args:
            datasource_name (str): The name of the datasource.
            id (str): The ID of the data object.
            data (Dict): The updated data for the object.

        Returns:
            Dict: The updated data object.
        """
        response = self.rest_client._patch(
            f"datasources/{datasource_name}/dataobjects/{id}", data=data
        )
        return response.data

    def create(self, datasource_name: str, data: Dict) -> Dict:
        """
        Create a new data object in a datasource.

        Args:
            datasource_name (str): The name of the datasource.
            data (Dict): The data for the new object.

        Returns:
            Dict: The created data object.
        """
        ValidateDataSource.create(data)

        response = self.rest_client._post(
            f"datasources/{datasource_name}/dataobjects", data=data
        )
        return response.data

    def upsert(self, datasource_name: str, data: List[Dict]) -> List[str]:
        """
        Upsert (insert or update) multiple data objects in a datasource.

        Args:
            datasource_name (str): The name of the datasource.
            data (List[Dict]): The data objects to upsert.

        Returns:
            List[str]: The IDs of the upserted data objects.
        """
        ValidateDataSource.upsert(data)

        response = self.rest_client._post(
            f"datasources/{datasource_name}/dataobjects/import", data=data
        )
        return response.data

    def delete(self, datasource_name: str, id: str) -> None:
        """
        Delete a specific data object from a datasource.

        Args:
            datasource_name (str): The name of the datasource.
            id (str): The ID of the data object.
        """
        self.rest_client._delete(f"datasources/{datasource_name}/dataobjects/{id}")
