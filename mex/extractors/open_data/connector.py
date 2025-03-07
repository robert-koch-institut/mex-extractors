import math
from collections.abc import Generator

from mex.common.connector import HTTPConnector
from mex.extractors.open_data.models.source import (
    OpenDataParentResource,
    OpenDataResourceVersion,
    OpenDataVersionFiles,
)
from mex.extractors.settings import Settings


class OpenDataConnector(HTTPConnector):
    """Connector class to handle requesting the Zenodo API."""

    def _set_url(self) -> None:
        """Set url of the host."""
        settings = Settings.get()
        self.url = settings.open_data.url
        self.community_rki = settings.open_data.community_rki

    def get_parent_resources(self) -> Generator[OpenDataParentResource, None, None]:
        """Load parent resources by querying the Zenodo API.

        Gets the parent resources (~ latest version) of all the resources of the
        configured Zenodo community.

        Returns:
            Generator for parent resources
        """
        parents_base_url = f"api/communities/{self.community_rki}/records?"
        total_records = self.request("GET", f"{parents_base_url}size=1")["hits"][
            "total"
        ]

        limit = 41  # limit = 100
        amount_pages = math.ceil(total_records / limit)

        for page in range(2, amount_pages + 1):  # range=1
            response = self.request(
                "GET",
                f"{parents_base_url}size={limit}&page={page}",
            )

            for item in response["hits"]["hits"]:
                yield OpenDataParentResource.model_validate(item)

    def get_resource_versions(
        self, resource_id: int
    ) -> Generator[OpenDataResourceVersion, None, None]:
        """Load versions of different parent resources by querying the Zenodo API.

        For a specific parent resource get all the versions of this resource.
        The Zenodo API doesn't work by giving it the parent resource id ("conceptrecid")
        but call it with the id ("id") of any version of that parent resource and then
        'ask' for the versions in general.

        Args:
            resource_id: id of any resource version

        Returns:
            Generator for Zenodo resource versions
        """
        versions_base_url = f"api/records/{resource_id}/versions?"

        total_records = self.request("GET", f"{versions_base_url}size=1")["hits"][
            "total"
        ]

        limit = 100
        amount_pages = math.ceil(total_records / limit)

        for page in range(1, amount_pages + 1):
            response = self.request(
                "GET",
                f"{versions_base_url}size={limit}&page={page}",
            )

            for item in response["hits"]["hits"]:
                yield OpenDataResourceVersion.model_validate(item)

    def get_oldest_resource_version_creationdate(self, resource_id: int) -> str | None:
        """Load oldest (first) version of a resource by querying the Zenodo API.

        Args:
            resource_id: id of any resource version

        Returns:
            Zenodo resource version (oldest)
        """
        versions_base_url = f"api/records/{resource_id}/versions?"

        oldest_record = self.request("GET", f"{versions_base_url}size=1&sort=oldest")

        item = oldest_record["hits"]["hits"][0]

        if oldest_record["hits"]["hits"][0]["metadata"]["publication_date"]:
            return OpenDataResourceVersion.model_validate(
                item
            ).metadata.publication_date
        return None

    def get_files_for_resource_version(
        self, version_id: int
    ) -> Generator[OpenDataVersionFiles, None, None]:
        """Load files for each version of a resource by querying the Zenodo API.

        Args:
            version_id: id of a resource version

        Returns:
            Zenodo resource version files
        """
        files_base_url = f"api/records/{version_id}/files"

        files = self.request("GET", files_base_url)

        for file in files["entries"]:
            yield OpenDataVersionFiles.model_validate(file)
