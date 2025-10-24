from collections.abc import Iterable

from pydantic import ValidationError

from mex.common.logging import logger
from mex.common.models import ActivityMapping, ExtractedActivity
from mex.common.types import (
    ActivityType,
    Link,
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
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.settings import Settings


def transform_confluence_vvt_page_to_extracted_activity(
    page: ConfluenceVvtPage,
    confluence_vvt_activity_mapping: ActivityMapping,
    merged_ids_by_query_string: dict[str, list[MergedPersonIdentifier]],
    unit_merged_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> ExtractedActivity | None:
    """Transform Confluence-vvt page to extracted activity.

    Args:
        page: Confluence-vvt page
        confluence_vvt_activity_mapping: activity mapping for confluence-vvt
        merged_ids_by_query_string: Mapping from author query to merged IDs
        unit_merged_ids_by_synonym: Map from unit acronyms and labels to their merged ID

    Returns:
        ExtractedActivity or None
    """
    settings = Settings.get()
    identifier_in_primary_source = page.get_identifier_in_primary_source()
    if not identifier_in_primary_source:
        return None

    if abstract_field := confluence_vvt_activity_mapping.abstract[
        0
    ].fieldInPrimarySource:
        abstract: str | None = "\n".join(
            page.tables[0].get_value_row_by_heading(abstract_field).cells[0].get_texts()
        )
    else:
        abstract = None

    contact_merged_ids = [
        merged_id
        for contact in get_contact_from_page(page, confluence_vvt_activity_mapping)
        for merged_id in merged_ids_by_query_string[contact]
    ]
    documentation = [
        Link(
            url=f"{settings.confluence_vvt.url}/pages/viewpage.action?pageId={page.id}",
            title=confluence_vvt_activity_mapping.documentation[0]  # type: ignore[index]
            .mappingRules[0]
            .setValues[0]
            .title,
        )
    ]
    involved_person_merged_ids = [
        merged_id
        for author in get_involved_persons_from_page(
            page, confluence_vvt_activity_mapping
        )
        for merged_id in merged_ids_by_query_string[author]
    ]
    responsible_unit_strings = get_responsible_unit_from_page(
        page, confluence_vvt_activity_mapping
    )
    for unit in responsible_unit_strings:
        if "ZV" in unit:  # stopgap: MX-1786
            return None
    responsible_unit_merged_ids = list(
        {
            unit_id
            for oe in responsible_unit_strings
            if (unit_id := unit_merged_ids_by_synonym.get(oe))
        }
    )
    involved_unit_merged_ids = list(
        {
            unit_id: None
            for oe in get_involved_units_from_page(
                page, confluence_vvt_activity_mapping
            )
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
        documentation=documentation,
        identifierInPrimarySource=identifier_in_primary_source,
        involvedPerson=involved_person_merged_ids,
        involvedUnit=involved_unit_merged_ids,
        responsibleUnit=responsible_unit_merged_ids or involved_unit_merged_ids,
        title=page.title,
        hadPrimarySource=get_extracted_primary_source_id_by_name("confluence-vvt"),
    )


def transform_confluence_vvt_activities_to_extracted_activities(
    pages: Iterable[ConfluenceVvtPage],
    confluence_vvt_activity_mapping: ActivityMapping,
    merged_ids_by_query_string: dict[str, list[MergedPersonIdentifier]],
    unit_merged_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> list[ExtractedActivity]:
    """Transform Confluence-vvt pages to extracted activities.

    Args:
        pages: All Confluence-vvt pages
        confluence_vvt_activity_mapping: activity mapping for confluence-vvt
        merged_ids_by_query_string: Mapping from author query to merged IDs
        unit_merged_ids_by_synonym: Map from unit acronyms and labels to their merged ID

    Returns:
        List of ExtractedActivity
    """
    extracted_activities = []
    for page in pages:
        try:
            extracted_activity = transform_confluence_vvt_page_to_extracted_activity(
                page,
                confluence_vvt_activity_mapping,
                merged_ids_by_query_string,
                unit_merged_ids_by_synonym,
            )
            if extracted_activity:
                extracted_activities.append(extracted_activity)
        except ValidationError as error:
            logger.info(f"failed on page {page.id} with {error}")
    return extracted_activities
