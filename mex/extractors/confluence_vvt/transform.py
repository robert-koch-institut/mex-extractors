from collections.abc import Iterable

from mex.common.models import ExtractedActivity, ExtractedPrimarySource
from mex.common.types import (
    ActivityType,
    MergedOrganizationalUnitIdentifier,
    MergedPersonIdentifier,
    Text,
    TextLanguage,
)
from mex.extractors.confluence_vvt.extract import (
    get_contact_from_page,
    get_involved_persons_from_page,
    get_involved_units_from_page,
    get_responsible_unit_from_page,
)
from mex.extractors.confluence_vvt.models import ConfluenceVvtPage
from mex.extractors.mapping.types import AnyMappingModel
from mex.extractors.settings import Settings


def transform_confluence_vvt_page_to_extracted_activity(
    page: ConfluenceVvtPage,
    extracted_primary_source: ExtractedPrimarySource,
    activity_mapping: AnyMappingModel,
    merged_ids_by_query_string: dict[str, list[MergedPersonIdentifier]],
    unit_merged_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> ExtractedActivity | None:
    """Transform Confluence-vvt page to extracted activity.

    Args:
        page: Confluence-vvt page
        extracted_primary_source: Extracted primary source for Confluence-vvt
        activity_mapping: activity mapping for confluence-vvt
        merged_ids_by_query_string: Mapping from author query to merged IDs
        unit_merged_ids_by_synonym: Map from unit acronyms and labels to their merged ID

    Returns:
        ExtractedActivity or None
    """
    settings = Settings.get()
    identifier_in_primary_source = page.get_identifier_in_primary_source()
    if not identifier_in_primary_source:
        return None

    abstract = "\n".join(
        page.tables[0]
        .get_value_by_heading(activity_mapping.abstract[0].fieldInPrimarySource)
        .cells[0]
        .get_texts()
    )

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

    if (
        not contact_merged_ids
        and not involved_person_merged_ids
        and not responsible_unit_merged_ids
    ):
        return None

    return ExtractedActivity(
        abstract=[Text(value=abstract, language=TextLanguage.DE)] if abstract else [],
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
    """Transform Confluence-vvt pages to extracted activities.

    Args:
        pages: All Confluence-vvt pages
        extracted_primary_source: Extracted primary source for Confluence-vvt
        activity_mapping: activity mapping for confluence-vvt
        merged_ids_by_query_string: Mapping from author query to merged IDs
        unit_merged_ids_by_synonym: Map from unit acronyms and labels to their merged ID

    Returns:
        List of ExtractedActivity
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
