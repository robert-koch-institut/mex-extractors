from typing import Any, cast
from urllib.parse import urljoin

from requests import Response

from mex.common.connector import HTTPConnector
from mex.extractors.settings import Settings


class DropApiConnector(HTTPConnector):
    """Connector class to handle interaction with the Drop API."""

    API_VERSION = "v0"

    def _check_availability(self) -> None:
        """Send a GET request to verify the API is available."""
        self.request("GET", "../_system/check")

    def _set_authentication(self) -> None:
        """Set the drop API key to all session headers."""
        settings = Settings.get()
        self.session.headers["X-API-Key"] = settings.drop_api_key.get_secret_value()

    def _set_url(self) -> None:
        """Set the drop api url with the version path."""
        settings = Settings.get()
        self.url = urljoin(str(settings.drop_api_url), self.API_VERSION)

    def list_files(self, x_system: str) -> list[str]:
        """Get available files for the x_system.

        Args:
            x_system: name of the x_system to list the files for

        Returns:
            list of available filenames for the x_system
        """
        response_json = self.request(method="GET", endpoint=f"/{x_system}")
        return cast("list[str]", response_json["entity-types"])

    def get_file(self, x_system: str, file_id: str) -> dict[str, Any]:
        """Get the content of a JSON file from the x_system.

        Args:
            x_system: name of the x_system
            file_id: name of the file

        Returns:
            content of the JSON file
        """
        return self.request(
            method="GET",
            endpoint=f"/{x_system}/{file_id}",
        )

    def get_raw_file(self, x_system: str, file_id: str) -> Response:
        """Get the raw content of a file from the x_system.

        Args:
            x_system: name of the x_system
            file_id: name of the file

        Returns:
            raw content of the file
        """
        return self.request_raw(
            method="GET",
            endpoint=f"/{x_system}/{file_id}",
        )
