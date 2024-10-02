from mex.common.connector import HTTPConnector
from mex.extractors.settings import Settings


class ConfluenceVvtConnector(HTTPConnector):
    """Connector class to create a session for all requests to confluence-vvt."""

    def _set_url(self) -> None:
        """Set url of the host."""
        settings = Settings.get()
        self.url = settings.confluence_vvt.url

    def _set_authentication(self) -> None:
        """Authenticate to the host."""
        settings = Settings.get()
        self.session.auth = (
            settings.confluence_vvt.username.get_secret_value(),
            settings.confluence_vvt.password.get_secret_value(),
        )
