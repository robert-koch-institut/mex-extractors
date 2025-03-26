from mex.extractors.open_data.connector import OpenDataConnector
from mex.extractors.open_data.models.source import (
    OpenDataParentResource,
    OpenDataResourceVersion,
    OpenDataVersionFiles,
)


def extract_parent_resources() -> list[OpenDataParentResource]:
    """Load Open Data resources by querying the Zenodo API.

    Get all resources of the configured Zenodo community.
    These are called 'parent resources'.

    Returns:
        list of parent resources
    """
    connector = OpenDataConnector()

    return connector.get_parent_resources()


def extract_resource_versions(
    open_data_parent_resources: list[OpenDataParentResource],
) -> list[OpenDataResourceVersion]:
    """Fetch all the versions of a parent resource.

    Args:
        open_data_parent_resources: Open Data rarent resource

    Returns:
        list of OpenDataResourceVersion items
    """
    connector = OpenDataConnector()

    return [
        resource_version
        for parent_resource in open_data_parent_resources
        for resource_version in connector.get_resource_versions(parent_resource.id)
    ]


def extract_oldest_record_version_creationdate(record_id: int) -> str | None:
    """Fetch only the oldest version of a parent resource.

    Args:
        record_id: id of record version as integer

    Returns:
        OpenDataResourceVersion
    """
    connector = OpenDataConnector()

    return connector.get_oldest_resource_version_creation_date(record_id)


def extract_files_for_parent_resource(
    version_id: int,
) -> list[OpenDataVersionFiles]:
    """Fetch all files of a version resource.

    Args:
        version_id: id of record version as integer

    Returns:
        OpenDataVersionFiles
    """
    connector = OpenDataConnector()

    return connector.get_files_for_resource_version(version_id)
