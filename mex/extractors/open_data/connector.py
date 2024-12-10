import math
from collections.abc import Generator
from urllib.parse import urljoin

from mex.common.connector import HTTPConnector
from mex.common.exceptions import MExError
from mex.extractors.open_data.models.source import (
    ZenodoParentRecordSource,
    ZenodoRecordVersion,
)
from mex.extractors.settings import Settings


class OpenDataConnector(HTTPConnector):
    """Connector class to handle requesting the Zenodo API."""

    def _set_url(self) -> None:
        """Set url of the host."""
        settings = Settings.get()
        self.url = settings.open_data.url

    def get_parent_sources(self) -> Generator[ZenodoParentRecordSource, None, None]:
        """Load parent sources by querying the Zenodo API.

        Returns:
            Generator for Zenodo parent sources
        """
        communities = "robertkochinstitut"

        base_url = urljoin(
            self.url,
            f"communities/{communities}/",
        )

        total_records = self.session.get(urljoin(base_url, "records?size=1")).json()[
            "hits"
        ]["total"]

        limit = 100
        amount_pages = math.ceil(total_records / limit)

        for page in range(1, amount_pages + 1):
            response = self.session.get(
                urljoin(
                    base_url,
                    f"records?size={limit}&page={page}",
                )
            )
            response.raise_for_status()

            for item in response.json()["hits"]["hits"]:
                yield ZenodoParentRecordSource.model_validate(item)
        msg = "Pagination limit reached to fetch all data pages list"
        raise MExError(msg)

    def get_record_versions(self) -> Generator[ZenodoRecordVersion, None, None]:
        """Load parent sources by querying the Zenodo API.

        Returns:
            Generator for Zenodo parent sources
        """
        record_id = 14229891

        base_url = urljoin(
            self.url,
            f"records/{record_id}/",
        )

        total_records = self.session.get(urljoin(base_url, "versions?size=1")).json()[
            "hits"
        ]["total"]

        limit = 100
        amount_pages = math.ceil(total_records / limit)

        for page in range(1, amount_pages + 1):
            response = self.session.get(
                urljoin(
                    base_url,
                    f"versions?size={limit}&page={page}",
                )
            )
            response.raise_for_status()

            for item in response.json()["hits"]["hits"]:
                yield ZenodoRecordVersion.model_validate(item)
        msg = "Pagination limit reached to fetch all data pages list"
        raise MExError(msg)

    def get_oldest_record_versions(self) -> ZenodoRecordVersion:
        """Load parent sources by querying the Zenodo API.

        Returns:
            Generator for Zenodo parent sources
        """
        record_id = 14229891

        base_url = urljoin(
            self.url,
            f"records/{record_id}/",
        )

        oldest_record = self.session.get(
            urljoin(base_url, "versions?size=1&sort=oldest")
        ).json()

        oldest_record.raise_for_status()

        item = oldest_record.json()["hits"]["hits"][0]

        return ZenodoRecordVersion.model_validate(item)
