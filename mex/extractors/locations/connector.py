from mex.common.connector.http import HTTPConnector
from mex.common.logging import logger
from mex.common.settings import BaseSettings
from mex.extractors.settings import ExtractorsSettings

LOCATION_QUERY = """
    SELECT DISTINCT ?item WHERE {
      VALUES ?locationClass {
        wd:Q1221156
        wd:Q14784328
        wd:Q22865
        wd:Q116457956
      }

      ?item wdt:P31/wdt:P279* ?locationClass.
    }
"""


class WikidataSPARQLConnector(HTTPConnector):
    """Connector class to handle requesting Wikidata with SPARQL."""

    def _set_url(self) -> None:
        """Set url of the host."""
        settings = ExtractorsSettings.get()
        self.url = str(settings.locations.sparql_url)

    def _set_session(self) -> None:
        """Create and set request session."""
        super()._set_session()
        settings = BaseSettings.get()
        self.session.headers.update(
            {
                "User-Agent": settings.mex_web_user_agent,
                "Api-User-Agent": settings.mex_web_user_agent,
            }
        )

    def extract_wikidata_location_ids(self) -> list[str]:
        """Retrieve location IDs from Wikidata via SPARQL.

        The query searches for locations defined by the Q-numbers of territories lists.
        """
        response = self.request(
            params={
                "query": LOCATION_QUERY,
                "format": "json",
            },
            timeout=60,
        )
        response.raise_for_status()

        bindings = response.json()["results"]["bindings"]

        logger.info(f"Extracted {len(bindings)} location IDs from wikidata")

        return sorted(
            {binding["item"]["value"].rsplit("/", 1)[-1] for binding in bindings}
        )
