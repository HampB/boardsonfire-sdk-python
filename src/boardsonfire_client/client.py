import requests
import logging

from json import JSONDecodeError
from dataclasses import dataclass
from typing import List, Dict

from .exceptions import RateLimitException, NotFoundException, RestClientException


@dataclass
class Response:
    status_code: int
    headers: Dict
    message: str = ""
    data: List[Dict] = None


class BoardsOnFireClient:
    """
    A client for interacting with the BoardsOnFire API.

    Args:
        domain (str): The domain of the BoardsOnFire instance.
        api_key (str): The API key for authentication.
        version (str, optional): The version of the API to use. Defaults to "v5".

    Attributes:
        domain (str): The domain of the BoardsOnFire instance.
        url (str): The base URL for API requests.
        api_key (str): The API key for authentication.
        version (str): The version of the API being used.
        organizations (Organizations): An instance of the Organizations class for interacting with organization-related endpoints.
        users (Users): An instance of the Users class for interacting with user-related endpoints.
        entities (Entities): An instance of the Entities class for interacting with entity-related endpoints.
        datasources (DataSources): An instance of the DataSources class for interacting with data source-related endpoints.
    """

    def __init__(
        self,
        domain: str,
        api_key: str,
        version: str = "v5",
        logger: logging.Logger = None,
    ):

        from .endpoints import Users, Organizations, Entities, DataSources

        self._logger = logger or logging.getLogger(__name__)

        self._domain = domain
        self._url = f"https://{domain}.boardsonfireapp.com/api/{version}/"
        self._api_key = api_key
        self._version = version

        self.organizations = Organizations(self)
        self.users = Users(self)
        self.entities = Entities(self)
        self.datasources = DataSources(self)

    def _send_request(
        self,
        method: str,
        endpoint: str,
        params: Dict = None,
        data: Dict = None,
        headers: Dict = {},
    ) -> Response:
        """
        Sends a request to the BoardsOnFire API.

        Args:
            method (str): The HTTP method for the request.
            endpoint (str): The API endpoint to send the request to.
            params (Dict, optional): The query parameters for the request. Defaults to None.
            data (Dict, optional): The request body data. Defaults to None.
            headers (Dict, optional): Additional headers for the request. Defaults to {}.

        Returns:
            Response: The response from the API.

        Raises:
            RateLimitException: If the rate limit is exceeded.
            NotFoundException: If the requested resource is not found.
            RestClientException: If the response is not successful or does not contain valid JSON.
        """

        full_url = self._url + endpoint
        base_header = {"x-api-key": self._api_key}
        headers = base_header | headers

        response = requests.request(
            method=method, url=full_url, headers=headers, params=params, json=data
        )

        if response.ok:
            if method == "DELETE":
                return Response(
                    status_code=response.status_code, headers=response.headers
                )

            try:
                data = response.json()
            except (ValueError, JSONDecodeError) as e:
                raise RestClientException("Response does not contain valid json")
            return Response(
                status_code=response.status_code, headers=response.headers, data=data
            )

        if response.status_code == 429:
            raise RateLimitException("Rate limit exceeded")
        if response.status_code == 404:
            raise NotFoundException("Resource not found")
        raise RestClientException(
            f"Bad response. \n Status Code: {response.status_code}\n  Message: {response.content}"
        )

    def _get(self, endpoint: str, params: Dict = None, data: Dict = None) -> Response:
        """
        Sends a GET request to the BoardsOnFire API.

        Args:
            endpoint (str): The API endpoint to send the request to.
            params (Dict, optional): The query parameters for the request. Defaults to None.
            data (Dict, optional): The request body data. Defaults to None.

        Returns:
            Response: The response from the API.

        Raises:
            RateLimitException: If the rate limit is exceeded.
            NotFoundException: If the requested resource is not found.
            RestClientException: If the response is not successful or does not contain valid JSON.
        """
        return self._send_request(
            method="GET", endpoint=endpoint, params=params, data=data
        )

    def _post(self, endpoint: str, params: Dict = None, data: Dict = None) -> Response:
        """
        Sends a POST request to the BoardsOnFire API.

        Args:
            endpoint (str): The API endpoint to send the request to.
            params (Dict, optional): The query parameters for the request. Defaults to None.
            data (Dict, optional): The request body data. Defaults to None.

        Returns:
            Response: The response from the API.

        Raises:
            RateLimitException: If the rate limit is exceeded.
            NotFoundException: If the requested resource is not found.
            RestClientException: If the response is not successful or does not contain valid JSON.
        """
        return self._send_request(
            method="POST", endpoint=endpoint, params=params, data=data
        )

    def _delete(
        self, endpoint: str, params: Dict = None, data: Dict = None
    ) -> Response:
        """
        Sends a DELETE request to the BoardsOnFire API.

        Args:
            endpoint (str): The API endpoint to send the request to.
            params (Dict, optional): The query parameters for the request. Defaults to None.
            data (Dict, optional): The request body data. Defaults to None.

        Returns:
            Response: The response from the API.

        Raises:
            RateLimitException: If the rate limit is exceeded.
            NotFoundException: If the requested resource is not found.
            RestClientException: If the response is not successful or does not contain valid JSON.
        """
        return self._send_request(
            method="DELETE", endpoint=endpoint, params=params, data=data
        )

    def _patch(self, endpoint: str, params: Dict = None, data: Dict = None) -> Response:
        """
        Sends a PATCH request to the BoardsOnFire API.

        Args:
            endpoint (str): The API endpoint to send the request to.
            params (Dict, optional): The query parameters for the request. Defaults to None.
            data (Dict, optional): The request body data. Defaults to None.

        Returns:
            Response: The response from the API.

        Raises:
            RateLimitException: If the rate limit is exceeded.
            NotFoundException: If the requested resource is not found.
            RestClientException: If the response is not successful or does not contain valid JSON.
        """
        return self._send_request(
            method="PATCH", endpoint=endpoint, params=params, data=data
        )
