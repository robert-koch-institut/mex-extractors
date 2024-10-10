from collections.abc import Generator, Hashable, Iterable

from mex.common.logging import watch
from mex.common.models import ExtractedActivity, ExtractedPrimarySource
from mex.common.types import ActivityType, Identifier
from mex.extractors.confluence_vvt.models import ConfluenceVvtSource


@watch
def transform_confluence_vvt_sources_to_extracted_activities(
    confluence_vvt_sources: Iterable[ConfluenceVvtSource],
    extracted_primary_source: ExtractedPrimarySource,
    merged_ids_by_query_string: dict[Hashable, list[Identifier]],
    unit_merged_ids_by_synonym: dict[str, Identifier],
) -> Generator[ExtractedActivity, None, None]:
    """Transform Confluence-vvt sources to extracted activities.

    Args:
        confluence_vvt_sources: Confluence-vvt sources
        extracted_primary_source: Extracted primary source for Confluence-vvt
        merged_ids_by_query_string: Mapping from author query to merged IDs
        unit_merged_ids_by_synonym: Map from unit acronyms and labels to their merged ID

    Returns:
        Generator for ExtractedActivity
    """
    for source in confluence_vvt_sources:
        if not source.identifier_in_primary_source:
            continue
        contact_merged_ids = [
            merged_id
            for author in source.contact
            for merged_id in merged_ids_by_query_string[author]
        ]
        responsible_unit_merged_ids = [
            unit_id
            for oe in source.responsible_unit
            if (unit_id := unit_merged_ids_by_synonym.get(oe))
        ]
        involved_person_merged_ids = [
            merged_id
            for author in source.involved_person
            for merged_id in merged_ids_by_query_string[author]
        ]
        involved_unit_merged_ids = [
            unit_id
            for oe in source.involved_unit
            if (unit_id := unit_merged_ids_by_synonym.get(oe))
        ]
        if not responsible_unit_merged_ids and not involved_unit_merged_ids:
            continue

        if (
            not contact_merged_ids
            and not involved_person_merged_ids
            and not responsible_unit_merged_ids
        ):
            continue

        yield ExtractedActivity(
            abstract=source.abstract,
            activityType=ActivityType["OTHER"],
            alternativeTitle=source.alternative_title,
            contact=contact_merged_ids
            or involved_person_merged_ids
            or responsible_unit_merged_ids,
            documentation=source.documentation,
            identifierInPrimarySource=";".join(source.identifier_in_primary_source),
            involvedPerson=involved_person_merged_ids,
            involvedUnit=involved_unit_merged_ids,
            responsibleUnit=responsible_unit_merged_ids or involved_unit_merged_ids,
            title=source.title,
            hadPrimarySource=extracted_primary_source.stableTargetId,
        )
