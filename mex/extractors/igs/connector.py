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

    def get_json_from_api(self) -> dict[str, Any]:
        """Get json from IGS Open Api.

        Raises:
            MExError if IGS Open API returns an error in the response body

        Returns:
            Parsed JSON body of the response
        """
        response = self.request("GET")

        if (
            response.get("status", {}).get("code", requests.codes["ok"])
            >= requests.codes["bad"]
        ):
            raise MExError(response)
        return response
