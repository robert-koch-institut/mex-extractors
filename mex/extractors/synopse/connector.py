from urllib.parse import urljoin

from requests import RequestException
from requests_ntlm import HttpNtlmAuth

from mex.common.connector import HTTPConnector
from mex.common.exceptions import MExError
from mex.extractors.settings import Settings


class ReportServerConnector(HTTPConnector):
    """Connector to handle authentication and requesting the Power BI Report Server."""

    # NOTE(ND) This connector is still unused because the report download currently
    # produces 500s and we need to work with the csv export until that is sorted.

    def _set_url(self) -> None:
        """Set url of the host."""
        settings = Settings.get()
        self.url = settings.synopse.report_server_url

    def _set_authentication(self) -> None:
        """Authenticate to the host."""
        settings = Settings.get()
        self.session.auth = HttpNtlmAuth(
            settings.synopse.report_server_username.get_secret_value(),
            settings.synopse.report_server_password.get_secret_value(),
        )

    def _check_availability(self) -> None:
        """Send a GET request to verify the host is available."""
        try:
            self.session.get(
                urljoin(self.url, "/reports/api/v2.0/"), timeout=self.TIMEOUT
            )
        except RequestException as error:
            msg = "Report server not available."
            raise MExError(msg) from error
