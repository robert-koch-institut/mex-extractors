from collections.abc import Generator

from mex.common.logging import watch
from mex.extractors.open_data.connector import OpenDataConnector
from mex.extractors.open_data.models.source import (
    OpenDataParentResource,
    OpenDataResourceVersion,
)


@watch
def extract_parent_resources() -> Generator[OpenDataParentResource, None, None]:
    """Load Open Data resources by querying the Zenodo API.

    Get all resources of  Zenodo community 'robertkochinstitut'.
    These are called 'parent resources'.

    Returns:
        Generator for parent resources
    """
    connector = OpenDataConnector()

    yield from connector.get_parent_resources()


@watch
def extract_resource_versions() -> Generator[OpenDataResourceVersion, None, None]:
    """Fetch all the versions of a parent resource.

    Returns:
        Generator for OpenDataResourceVersion items
    """
    connector = OpenDataConnector()

    for parent_resource in extract_parent_resources():
        if parent_resource.id:
            yield from connector.get_resource_versions(parent_resource.id)


def extract_oldest_record_version(record_id: int) -> OpenDataResourceVersion:
    """Fetch only the oldest version of a parent resource.

    Returns:
        OpenDataResourceVersion
    """
    connector = OpenDataConnector()

    return connector.get_oldest_resource_version(record_id)
