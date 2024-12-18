from mex.extractors.open_data.extract import (
    extract_parent_resources,
    extract_resource_versions,
)
from mex.extractors.open_data.models.source import (
    OpenDataParentResource,
    OpenDataResourceVersion,
)
from mex.extractors.pipeline import asset


@asset(group_name="open_data")
def open_data_parent_resources() -> list[OpenDataParentResource]:
    """Extract open data parent resources from Zenodo."""
    return list(extract_parent_resources())


@asset(group_name="open_data")
def open_data_resource_versions() -> list[OpenDataResourceVersion]:
    """Extract all versions of the parent resources from Zenodo."""
    return list(extract_resource_versions())