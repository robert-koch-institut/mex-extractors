from mex.common.connector.http import HTTPConnector
from mex.common.logging import logger
from mex.common.settings import BaseSettings
from mex.extractors.locations.query import build_wikidata_query
from mex.extractors.settings import ExtractorsSettings

REGION_TYPE_IDS = (  # Types of regions in Germany
    "Q1221156",  # federated state (Bundesland)
    "Q106658",  # district (Landkreis)
    "Q14784328",  # state capital (Landeshauptstadt)
    "Q22865",  # independent city (kreisfreie Stadt)
    "Q116457956",  # municipality without town privileges (Gemeinde)
)


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

        The query searches for locations defined by the Q-number list of territories.
        """
        location_ids: set[str] = set()
        for region_type_id in REGION_TYPE_IDS:
            response = self.request(
                "GET",
                params={
                    "query": build_wikidata_query(region_type_id),
                    "format": "json",
                },
                timeout=60,
            )
            bindings = response["results"]["bindings"]
            logger.info(
                "Extracted %d location IDs for %s from wikidata",
                len(bindings),
                region_type_id,
            )
            location_ids.update(
                binding["item"]["value"].rsplit("/", 1)[-1] for binding in bindings
            )
        logger.info("Extracted %d unique Wikidata location IDs", len(location_ids))
        return sorted(location_ids)
