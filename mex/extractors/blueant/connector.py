from collections.abc import Generator
from typing import Any
from urllib.parse import urljoin

import requests

from mex.common.connector import HTTPConnector
from mex.common.exceptions import MExError
from mex.extractors.blueant.models.person import BlueAntPerson, BlueAntPersonResponse
from mex.extractors.blueant.models.project import BlueAntProject, BlueAntProjectResponse
from mex.extractors.settings import Settings


class BlueAntConnector(HTTPConnector):
    """Connector class to handle authentication and requesting the Blue Ant API."""

    def _set_url(self) -> None:
        """Set url of the host."""
        settings = Settings.get()
        self.url = urljoin(settings.blueant.url, "rest/v1/")

    def _set_authentication(self) -> None:
        """Authenticate to the host."""
        settings = Settings.get()
        api_key = settings.blueant.api_key.get_secret_value()
        self.session.headers["Authorization"] = f"Bearer {api_key}"

    def _get_json_from_api(self, relative_url: str) -> dict[str, Any]:
        """Get json from blueant api.

        Args:
            relative_url (str): relative url of the api

        Raises:
            MExError if Blue Ant API returns an error in the response body

        Returns:
            Parsed JSON body of the response
        """
        response = self.request("GET", relative_url)
        if (
            response.get("status", {}).get("code", requests.codes["ok"])
            >= requests.codes["bad"]
        ):
            raise MExError(response)
        return response

    def get_projects(self) -> Generator[BlueAntProject, None, None]:
        """Load Blue Ant sources by querying the Blue Ant API projects endpoint.

        Returns:
            Generator for Blue Ant projects
        """
        dct = self._get_json_from_api("projects?includeArchived=true")

        yield from BlueAntProjectResponse.model_validate(dct).projects

    def get_client_name(self, client_id: int) -> str:
        """Get client name for client id.

        Args:
            client_id: int: id of the client

        Returns:
            str: name of the client
        """
        dct = self._get_json_from_api(f"masterdata/customers/{client_id}")
        return str(dct["customer"]["text"])

    def get_type_description(self, type_id: int) -> str:
        """Get description for type id.

        Args:
            type_id: int: id of the type

        Returns:
            str: description of the type
        """
        dct = self._get_json_from_api(f"masterdata/projects/types/{type_id}")
        return str(dct["type"]["description"])

    def get_status_name(self, status_id: int) -> str:
        """Get name for status id.

        Args:
            status_id: int: id of the status

        Returns:
            str: name of the status
        """
        dct = self._get_json_from_api(f"masterdata/projects/statuses/{status_id}")
        return str(dct["projectStatus"]["text"])

    def get_department_name(self, department_id: int) -> str:
        """Get name for department id.

        Args:
            department_id: int: id of the department

        Returns:
            str: name of the department
        """
        dct = self._get_json_from_api(f"masterdata/departments/{department_id}")
        return str(dct["department"]["text"])

    def get_persons(self) -> list[BlueAntPerson]:
        """Get map of Blue Ant person IDs to employee IDs."""
        dct = self._get_json_from_api("human/persons")
        return BlueAntPersonResponse.model_validate(dct).persons
