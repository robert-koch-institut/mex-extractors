from urllib.parse import urljoin

from bs4 import BeautifulSoup

from mex.common.connector import HTTPConnector
from mex.extractors.confluence_vvt.models import ConfluenceVvtPage
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

    def get_page_by_id(self, page_id: str) -> ConfluenceVvtPage | None:
        """Get confluence page data by its id.

        Args:
            page_id: confluence page id

        Returns:
            ConfluenceVvtPage or None
        """
        settings = Settings.get()
        if page_id in settings.confluence_vvt.skip_pages:
            return None

        page_labels = self.session.get(
            urljoin(
                settings.confluence_vvt.url,
                f"rest/api/content/{page_id}/label",
            ),
        )
        for label in page_labels.json()["results"]:
            if "vvt-template-v2" in label["name"]:
                return None

        response = self.session.get(
            urljoin(
                settings.confluence_vvt.url,
                f"rest/api/content/{page_id}?expand=body.view",
            ),
        )
        response.raise_for_status()
        json_data = response.json()

        html = json_data["body"]["view"]["value"]
        title = json_data["title"]
        soup = BeautifulSoup(html, "html.parser")
        tables = []

        for table in soup.find_all("table", {"class": "confluenceTable"}):
            rows = []
            for row in table.find_all("tr"):
                cells = [
                    {"text": header.get_text().strip() or None}
                    for header in row.find_all("th")
                ]
                for value in row.find_all("td"):
                    texts = [
                        text
                        for child in value.children
                        if (text := child.get_text().strip())
                    ]
                    cells.append({"texts": texts or None})
                rows.append({"cells": cells})
            tables.append({"rows": rows})
        return ConfluenceVvtPage.model_validate(
            {"title": title, "id": page_id, "tables": tables}
        )
