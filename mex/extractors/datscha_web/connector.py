from typing import TYPE_CHECKING
from urllib.parse import urljoin

from mex.common.connector import HTTPConnector
from mex.common.exceptions import MExError
from mex.extractors.datscha_web.parse_html import (
    parse_item_urls_from_overview_html,
    parse_single_item_html,
)
from mex.extractors.settings import Settings

if TYPE_CHECKING:
    from mex.extractors.datscha_web.models.item import DatschaWebItem

MAX_ITEMS_TO_FETCH = 999


class DatschaWebConnector(HTTPConnector):
    """Connector class to handle credentials and parsing of datscha web registry."""

    def _set_url(self) -> None:
        """Set url of the host."""
        settings = Settings.get()
        self.url = settings.datscha_web.url

    def _set_authentication(self) -> None:
        """Authenticate to the host."""
        settings = Settings.get()
        credentials = {
            "vorname": settings.datscha_web.vorname.get_secret_value(),
            "nachname": settings.datscha_web.nachname.get_secret_value(),
            "pw": settings.datscha_web.pw.get_secret_value(),
            "organisation": settings.datscha_web.organisation,
        }

        response = self.session.post(
            urljoin(self.url, "login.php"), data=credentials, timeout=1
        )
        if response.url != urljoin(self.url, "verzeichnis.php"):
            msg = (
                f"Datscha web login failed. Did you provide correct credentials for "
                f"{list(map(str.upper, credentials.keys()))}?"
            )
            raise MExError(msg)

    def get_item_urls(self) -> list[str]:
        """Accumulate datscha item URLs by scraping the page `verzeichnis.php`.

        Returns:
            List of item URLs
        """
        form_data = {
            "navigation": "anzeigen",
            "start": "1",
            "anzahl_ds": str(MAX_ITEMS_TO_FETCH),
            "suche_bezeichnung": "",
            "suche_kategorien_id": "0",
            "suche_stand": "",
        }
        response = self.session.post(
            urljoin(self.url, "verzeichnis.php"), data=form_data, timeout=1
        )
        item_urls = parse_item_urls_from_overview_html(response.text, self.url)
        if items_found := len(item_urls) >= MAX_ITEMS_TO_FETCH:
            # TODO(ND): we need to implement pagination if we ever run into this.
            msg = f"Found more items than I am able to handle: {items_found}"
            raise MExError(msg)
        return item_urls

    def get_item(self, item_url: str) -> DatschaWebItem:
        """Load and parse a single datscha item from the given URL.

        Args:
            item_url: URL of the item's datscha page

        Returns:
            DatschaWebItem: Parsed datscha item
        """
        response = self.session.get(item_url, timeout=1)
        return parse_single_item_html(response.text, item_url)
