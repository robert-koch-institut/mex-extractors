from typing import Any

import requests

from mex.common.connector import HTTPConnector
from mex.common.exceptions import MExError
from mex.extractors.settings import Settings


class IGSConnector(HTTPConnector):
    """Connector class to handle requesting IGS OpenAPI."""

    def _set_url(self) -> None:
        """Set url of the host."""
        settings = Settings.get()
        self.url = settings.igs.url

    def _check_availability(self) -> None:
        """Send a GET request to verify the API is available."""
        self.request("GET", endpoint="openapi.json")

    def get_json_from_api(self) -> dict[str, Any]:
        """Get json from IGS Open Api.

        Raises:
            MExError if IGS Open API returns an error in the response body

        Returns:
            Parsed JSON body of the response
        """
        response = self.request("GET", endpoint="openapi.json")

        if (
            response.get("status", {}).get("code", requests.codes["ok"])
            >= requests.codes["bad"]
        ):
            raise MExError(response)
        return response

    def get_endpoint_count(
        self, endpoint: str, params: dict[str, Any] | None = None
    ) -> str:
        """Get count from IGS by endpoint.

        Args:
            endpoint: endpoint to request
            params: dict of request params

        Returns:
            Parsed count str
        """
        response = self.request_raw("GET", endpoint=endpoint, params=params)

        return response.text
