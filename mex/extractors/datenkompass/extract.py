from typing import TYPE_CHECKING

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.exceptions import MExError
from mex.common.identity import get_provider
from mex.common.models import MEX_PRIMARY_SOURCE_STABLE_TARGET_ID

if TYPE_CHECKING:
    from mex.common.models import AnyMergedModel
    from mex.common.types import MergedIdentifier


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


def get_extracted_item_stable_target_ids(
    entity_type: list[str],
) -> list[MergedIdentifier]:
    """Fetch extracted items from backend and return their stableTargetId.

    Args:
        entity_type: List of entity types.

    Returns:
        List of stableTargetIds of extracted items of the given entity type(s).
    """
    connector = BackendApiConnector.get()

    response = connector.fetch_extracted_items(
        entity_type=entity_type,
        skip=0,
        limit=1,
    )
    total_item_number = response.total
    item_number_limit = 100  # 100 is the maximum possible number per get-request

    extracted_items = []
    for item_counter in range(0, total_item_number, item_number_limit):
        extracted_items.extend(
            connector.fetch_extracted_items(
                entity_type=entity_type,
                skip=item_counter,
                limit=item_number_limit,
            ).items
        )

    return [item.stableTargetId for item in extracted_items]


def get_filtered_primary_source_ids(
    filtered_primary_sources: list[str] | str | None,
) -> list[str]:
    """Get the MergedIdentifier of the relevant primary sources.

    Args:
        filtered_primary_sources: List of primary sources.

    Returns:
        List of IDs of the filtered relevant primary sources.
    """
    msg = "Primary sources not found."
    if not filtered_primary_sources:
        raise MExError(msg)

    if isinstance(filtered_primary_sources, str):
        filtered_primary_sources = [filtered_primary_sources]

    provider = get_provider()

    return [
        mps.stableTargetId
        for fps in filtered_primary_sources
        for mps in provider.fetch(
            identifier_in_primary_source=fps,
            had_primary_source=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
        )[:1]
    ]
