from typing import cast

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.exceptions import MExError
from mex.common.identity import get_provider
from mex.common.models import AnyMergedModel, MergedPrimarySource


def get_merged_items(
    *,
    query_string: str | None = None,
    entity_type: list[str] | None = None,
    referenced_identifier: list[str] | None = None,
    reference_field: str | None = None,
) -> list[AnyMergedModel]:
    """Fetch merged items from backend.

    Args:
        query_string: Query string.
        entity_type: List of entity types.
        referenced_identifier: List of Identifier.
        reference_field: List of fields accepting identifiers.

    Returns:
        List of merged items.
    """
    connector = BackendApiConnector.get()

    response = connector.fetch_all_merged_items(
        query_string=query_string,
        entity_type=entity_type,
        referenced_identifier=referenced_identifier,
        reference_field=reference_field,
    )

    return list(response)


def get_relevant_primary_source_ids(
    relevant_primary_sources: list[str] | None,
) -> list[str]:
    """Get the IDs of the relevant primary sources.

    Args:
        relevant_primary_sources: List of primary sources.

    Returns:
        List of IDs of the relevant primary sources.
    """
    msg = "Primary sources not found."
    if not relevant_primary_sources:
        raise MExError(msg)

    merged_primary_sources = cast(
        "list[MergedPrimarySource]",
        get_merged_items(entity_type=["MergedPrimarySource"]),
    )

    provider = get_provider()

    return [
        str(mps.identifier)
        for mps in merged_primary_sources
        if provider.fetch(stable_target_id=mps.identifier)[0].identifierInPrimarySource
        in relevant_primary_sources
    ]
