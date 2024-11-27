from collections.abc import Generator, Hashable, Iterable

from mex.common.logging import watch
from mex.common.models import ExtractedActivity, ExtractedPrimarySource
from mex.common.types import (
    ActivityType,
    Identifier,
    MergedPersonIdentifier,
    Text,
    TextLanguage,
    MergedOrganizationalUnitIdentifier,
)
from mex.extractors.confluence_vvt.models import ConfluenceVvtPage
from mex.extractors.mapping.types import AnyMappingModel
from mex.extractors.confluence_vvt.extract import (
    get_involved_persons_from_page,
    get_contact_from_page,
    get_responsible_unit_from_page,
    get_involved_units_from_page,
)
from mex.extractors.settings import Settings


def transform_confluence_vvt_page_to_extracted_activity(
    page: ConfluenceVvtPage,
    extracted_primary_source: ExtractedPrimarySource,
    activity_mapping: AnyMappingModel,
    merged_ids_by_query_string: dict[str, list[MergedPersonIdentifier]],
    unit_merged_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> ExtractedActivity | None:
    settings = Settings.get()
    identifier_in_primary_source = page.get_identifier_in_primary_source()
    if not identifier_in_primary_source:
        return None

    contact_merged_ids = [
        merged_id
        for contact in get_contact_from_page(page, activity_mapping)
        for merged_id in merged_ids_by_query_string[contact]
    ]
    involved_person_merged_ids = [
        merged_id
        for author in get_involved_persons_from_page(page, activity_mapping)
        for merged_id in merged_ids_by_query_string[author]
    ]
    responsible_unit_merged_ids = list(
        {
            unit_id
            for oe in get_responsible_unit_from_page(page, activity_mapping)
            if (unit_id := unit_merged_ids_by_synonym.get(oe))
        }
    )
    involved_unit_merged_ids = list(
        {
            unit_id
            for oe in get_involved_units_from_page(page, activity_mapping)
            if (unit_id := unit_merged_ids_by_synonym.get(oe))
        }
    )

    return ExtractedActivity(
        abstract=Text(
            value="\n".join(
                page.tables[0]
                .get_value_by_heading(activity_mapping.abstract[0].fieldInPrimarySource)
                .cells[0]
                .get_texts()
            ),
            language=TextLanguage.DE,
        ),
        activityType=ActivityType["OTHER"],
        contact=contact_merged_ids
        or involved_person_merged_ids
        or responsible_unit_merged_ids,
        documentation=f"{settings.confluence_vvt.url}/pages/viewpage.action?pageId={page.id}",
        identifierInPrimarySource=identifier_in_primary_source,
        involvedPerson=involved_person_merged_ids,
        involvedUnit=involved_unit_merged_ids,
        responsibleUnit=responsible_unit_merged_ids or involved_unit_merged_ids,
        title=page.title,
        hadPrimarySource=extracted_primary_source.stableTargetId,
    )


def transform_confluence_vvt_activities_to_extracted_activities(
    pages: Iterable[ConfluenceVvtPage],
    extracted_primary_source: ExtractedPrimarySource,
    activity_mapping: AnyMappingModel,
    merged_ids_by_query_string: dict[str, list[MergedPersonIdentifier]],
    unit_merged_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> list[ExtractedActivity]:
    """Transform Confluence-vvt sources to extracted activities.

    Args:
        confluence_vvt_sources: Confluence-vvt sources
        extracted_primary_source: Extracted primary source for Confluence-vvt
        merged_ids_by_query_string: Mapping from author query to merged IDs
        unit_merged_ids_by_synonym: Map from unit acronyms and labels to their merged ID

    Returns:
        Generator for ExtractedActivity
    """
    extracted_activities = []

    for page in pages:
        extracted_activity = transform_confluence_vvt_page_to_extracted_activity(
            page,
            extracted_primary_source,
            activity_mapping,
            merged_ids_by_query_string,
            unit_merged_ids_by_synonym,
        )
        if extracted_activity:
            extracted_activities.append(extracted_activity)

    return extracted_activities
