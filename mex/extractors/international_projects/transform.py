from collections.abc import Generator, Hashable, Iterable
from typing import Any

from mex.common.logging import watch
from mex.common.models import (
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedPrimarySource,
)
from mex.common.types import (
    Link,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
    Text,
    Theme,
)
from mex.extractors.international_projects.models.source import (
    InternationalProjectsSource,
)
from mex.extractors.sinks import load


def transform_international_projects_source_to_extracted_activity(
    source: InternationalProjectsSource,
    international_projects_activity: dict[str, Any],
    extracted_primary_source: ExtractedPrimarySource,
    person_stable_target_ids_by_query_string: dict[
        Hashable, list[MergedPersonIdentifier]
    ],
    unit_stable_target_id_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    funding_sources_stable_target_id_by_query: dict[str, MergedOrganizationIdentifier],
    partner_organizations_stable_target_id_by_query: dict[
        str, MergedOrganizationIdentifier
    ],
) -> ExtractedActivity | None:
    """Transform international projects source to extracted activity.

    Args:
        source: international projects sources
        international_projects_activity: extracted activity for default
                                         values from mapping
        extracted_primary_source: Extracted primary_source for FF Projects
        person_stable_target_ids_by_query_string: Mapping from author query
                                                  to person stable target ID
        unit_stable_target_id_by_synonym: Mapping from unit acronyms and labels
                                          to unit stable target ID
        funding_sources_stable_target_id_by_query: Mapping from funding sources
                                                   to organization stable target ID
        partner_organizations_stable_target_id_by_query: Mapping from partner orgs to
                                                         their stable target ID

    Returns:
        ExtractedActivity or None if it was filtered out
    """
    if not source.full_project_name and not source.project_abbreviation:
        return None
    project_leads = person_stable_target_ids_by_query_string.get(
        source.project_lead_person, []
    )
    project_lead_rki_unit = unit_stable_target_id_by_synonym.get(
        source.project_lead_rki_unit
    )

    if not project_lead_rki_unit:
        return None

    additional_rki_units = (
        unit_stable_target_id_by_synonym.get(source.additional_rki_units)
        if source.additional_rki_units
        else None
    )

    all_funder_or_commissioner: list[MergedOrganizationIdentifier] = []
    if source.funding_source:
        all_funder_or_commissioner.extend(
            wfc
            for fc in source.funding_source
            if (wfc := funding_sources_stable_target_id_by_query.get(fc))
        )

    all_partner_organizations: list[MergedOrganizationIdentifier] = []
    if source.partner_organization:
        for partner_org in source.partner_organization:
            if wpo := partner_organizations_stable_target_id_by_query.get(partner_org):
                all_partner_organizations.append(wpo)
            else:
                extracted_organization = ExtractedOrganization(
                    officialName=[Text(value=partner_org)],
                    identifierInPrimarySource=partner_org,
                    hadPrimarySource=extracted_primary_source.stableTargetId,
                )
                load([extracted_organization])
                all_partner_organizations.append(
                    MergedOrganizationIdentifier(extracted_organization.stableTargetId)
                )

    activity_type_from_mapping = international_projects_activity["activityType"][0][
        "mappingRules"
    ]
    if source.funding_type == activity_type_from_mapping[0]["forValues"][0]:
        activity_type = activity_type_from_mapping[0]["setValues"][0]
    elif source.funding_type == activity_type_from_mapping[1]["forValues"][0]:
        activity_type = activity_type_from_mapping[1]["setValues"][0]
    else:
        activity_type = activity_type_from_mapping[2]["setValues"][0]

    theme = international_projects_activity["theme"]
    return ExtractedActivity(
        title=source.full_project_name,
        activityType=activity_type,
        alternativeTitle=source.project_abbreviation,
        contact=[*project_leads, project_lead_rki_unit],
        involvedPerson=project_leads,
        involvedUnit=additional_rki_units,
        start=source.start_date,
        end=source.end_date,
        responsibleUnit=project_lead_rki_unit,
        identifierInPrimarySource=source.rki_internal_project_number
        or source.project_abbreviation,
        funderOrCommissioner=all_funder_or_commissioner,
        externalAssociate=all_partner_organizations,
        hadPrimarySource=extracted_primary_source.stableTargetId,
        fundingProgram=source.funding_program if source.funding_program else [],
        shortName=source.project_abbreviation,
        theme=get_theme_for_activity_or_topic(
            theme, source.activity1, source.activity2, source.topic1, source.topic2
        ),
        website=(
            []
            if source.website in ("", "does not exist yet")
            else [Link(url=source.website)]
        ),
    )


@watch
def transform_international_projects_sources_to_extracted_activities(
    international_projects_sources: Iterable[InternationalProjectsSource],
    international_projects_activity: dict[str, Any],
    extracted_primary_source: ExtractedPrimarySource,
    person_stable_target_ids_by_query_string: dict[
        Hashable, list[MergedPersonIdentifier]
    ],
    unit_stable_target_id_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    funding_sources_stable_target_id_by_query: dict[str, MergedOrganizationIdentifier],
    partner_organizations_stable_target_id_by_query: dict[
        str, MergedOrganizationIdentifier
    ],
) -> Generator[ExtractedActivity, None, None]:
    """Transform international projects sources to extracted activity.

    Args:
        international_projects_sources: international projects sources
        international_projects_activity: extracted activity for default
                                         values from mapping
        extracted_primary_source: Extracted primary_source for FF Projects
        person_stable_target_ids_by_query_string: Mapping from author query
                                                  to person stable target ID
        unit_stable_target_id_by_synonym: Mapping from unit acronyms and labels
                                          to unit stable target ID
        funding_sources_stable_target_id_by_query: Mapping from funding sources
                                                   to organization stable target ID
        partner_organizations_stable_target_id_by_query: Mapping from partner orgs to
                                                         their stable target ID

    Returns:
        Generator for ExtractedActivity instances
    """
    for source in international_projects_sources:
        if activity := transform_international_projects_source_to_extracted_activity(
            source,
            international_projects_activity,
            extracted_primary_source,
            person_stable_target_ids_by_query_string,
            unit_stable_target_id_by_synonym,
            funding_sources_stable_target_id_by_query,
            partner_organizations_stable_target_id_by_query,
        ):
            yield activity


def get_theme_for_activity_or_topic(
    theme: list[dict[str, Any]],
    activity1: str | None,
    activity2: str | None,
    topic1: str | None,
    topic2: str | None,
) -> list[Theme]:
    """Get theme identifier for activities and topics.

    Args:
        theme: theme extracted from mapping
        activity1: activity 1
        activity2: activity 1
        topic1: topic 1
        topic2: topic 2

    Returns:
        Sorted list of Theme
    """
    default_theme_from_mapping: Theme = theme[0]["mappingRules"][0]["setValues"][0]
    themes_dict_from_mapping: dict[str, Theme] = {}
    for theme_item in theme:
        for rule in theme_item["mappingRules"]:
            themes_dict_from_mapping.update(
                dict.fromkeys(rule["forValues"], rule["setValues"][0])
            )

    def get_theme_or_default(key: str | None) -> Theme:
        if key:
            if theme := themes_dict_from_mapping.get(key):
                return theme

        return default_theme_from_mapping

    theme_set = set()
    theme_set.add(get_theme_or_default(activity1))
    theme_set.add(get_theme_or_default(activity2))
    theme_set.add(get_theme_or_default(topic1))
    theme_set.add(get_theme_or_default(topic2))

    return sorted(list(theme_set), key=lambda x: x.name)
