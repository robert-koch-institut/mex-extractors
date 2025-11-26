import json
from io import BytesIO, TextIOWrapper
from zipfile import ZipFile

from mex.extractors.open_data.connector import OpenDataConnector
from mex.extractors.open_data.models.source import (
    OpenDataCreatorsOrContributors,
    OpenDataParentResource,
    OpenDataTableSchema,
    OpenDataTableSchemaJson,
    OpenDataVersionFiles,
)


def extract_parent_resources() -> list[OpenDataParentResource]:
    """Load Open Data resources by querying the Zenodo API.

    Get all resources of the configured Zenodo community.
    These are called 'parent resources'.

    Returns:
        list of parent resources
    """
    connector = OpenDataConnector.get()

    return connector.get_parent_resources()


def extract_oldest_record_version_creationdate(record_id: int) -> str | None:
    """Fetch only the oldest version of a parent resource.

    Args:
        record_id: id of record version as integer

    Returns:
        OpenDataResourceVersion
    """
    connector = OpenDataConnector.get()

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
    connector = OpenDataConnector.get()

    return connector.get_files_for_resource_version(version_id)


def extract_open_data_persons_from_open_data_parent_resources(
    open_data_parent_resource: list[OpenDataParentResource],
) -> list[OpenDataCreatorsOrContributors]:
    """Extract unique open Data persons from open data parent resources.

    Args:
        open_data_parent_resource: open data parent resource

    Returns:
        list of extracted open data persons (creators or contributors)
    """
    return list(
        {
            person: None
            for resource in open_data_parent_resource
            for person in (resource.metadata.creators + resource.metadata.contributors)
        }
    )


def extract_tableschema(version_id: int) -> dict[str, list[OpenDataTableSchema]]:
    """Extract the metadata zip tableschemas.

    Args:
        version_id: id of record version as integer

    Returns:
        name of tableschema json as string
    """
    connector = OpenDataConnector.get()

    zip_file = connector.get_schema_zipfile(version_id)

    schema_collection: dict[str, list[OpenDataTableSchema]] = {}
    with ZipFile(BytesIO(zip_file.content)) as zf:
        schema_file_paths = [
            n
            for n in zf.namelist()
            if (
                n.lower().startswith("metadaten/schemas/tableschema_")
                or n.lower().startswith("schemas/tableschema_")
            )
            and n.lower().endswith(".json")
        ]
        for file_path in schema_file_paths:
            with zf.open(file_path) as f:
                data = OpenDataTableSchemaJson.model_validate(
                    json.load(TextIOWrapper(f, encoding="utf-8"))
                )
            schema_collection[file_path.split("schemas/", 1)[1]] = data.fields

    return schema_collection
