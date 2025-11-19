from collections.abc import Iterable
from typing import cast

from mex.common.models import (
    ActivityMapping,
    ExtractedActivity,
    ExtractedOrganization,
    MappingField,
)
from mex.common.types import (
    Link,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
    Text,
    TextLanguage,
    Theme,
)
from mex.extractors.international_projects.models.source import (
    InternationalProjectsSource,
)
from mex.extractors.logging import watch_progress
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.sinks import load


def transform_international_projects_source_to_extracted_activity(  # noqa: PLR0913
    source: InternationalProjectsSource,
    international_projects_activity: ActivityMapping,
    person_stable_target_ids_by_query_string: dict[str, list[MergedPersonIdentifier]],
    unit_stable_target_id_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    funding_sources_stable_target_id_by_query: dict[str, MergedOrganizationIdentifier],
    partner_organizations_stable_target_id_by_query: dict[
        str, MergedOrganizationIdentifier
    ],
) -> ExtractedActivity | None:
    """Transform international projects source to extracted activity.

    Args:
        source: international projects sources
        international_projects_activity: activity mapping model with default values
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

    project_lead = [
        found_person[0]
        for person in source.get_project_lead_persons()
        if (
            found_person := person_stable_target_ids_by_query_string.get(person.strip())
        )
    ]

    project_lead_rki_unit = []
    for unit in source.get_project_lead_rki_units():
        if unit == "ZIG-GS":
            unit = "zig"  # noqa: PLW2901
        if found_unit := unit_stable_target_id_by_synonym.get(unit):
            project_lead_rki_unit.append(found_unit)

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
            for fc in source.get_funding_sources()
            if (wfc := funding_sources_stable_target_id_by_query.get(fc))
        )

    activity_type_from_mapping = international_projects_activity.activityType[
        0
    ].mappingRules
    if source.funding_type == activity_type_from_mapping[0].forValues[0]:  # type: ignore[index]
        activity_type = activity_type_from_mapping[0].setValues[0]  # type: ignore[index]
    elif source.funding_type == activity_type_from_mapping[1].forValues[0]:  # type: ignore[index]
        activity_type = activity_type_from_mapping[1].setValues[0]  # type: ignore[index]
    else:
        activity_type = activity_type_from_mapping[2].setValues[0]  # type: ignore[index]

    theme = international_projects_activity.theme

    return ExtractedActivity(
        title=[Text(language=TextLanguage.EN, value=source.full_project_name)],
        activityType=activity_type,
        alternativeTitle=source.project_abbreviation,
        contact=[*project_lead, *project_lead_rki_unit],
        involvedPerson=project_lead,
        involvedUnit=additional_rki_units,
        start=source.start_date,
        end=source.end_date,
        responsibleUnit=project_lead_rki_unit,
        identifierInPrimarySource=source.rki_internal_project_number.replace("\n", "/")
        or source.project_abbreviation,
        funderOrCommissioner=all_funder_or_commissioner,
        externalAssociate=(
            get_or_create_partner_organization(
                source.partner_organization,
                partner_organizations_stable_target_id_by_query,
            )
            if source.partner_organization
            else []
        ),
        hadPrimarySource=get_extracted_primary_source_id_by_name(
            "international-projects"
        ),
        fundingProgram=source.funding_program if source.funding_program else [],
        shortName=source.project_abbreviation,
        theme=get_theme_for_activity_or_topic(
            theme, source.activity1, source.activity2, source.topic1, source.topic2
        ),
        website=(
            []
            if not source.website
            or source.website
            in (
                "",
                "None",
                "does not exist yet",
                "does not exist",
                "will be on GO4BSB Platform",
            )
            else [Link(url=source.website)]
        ),
    )


def transform_international_projects_sources_to_extracted_activities(  # noqa: PLR0913
    international_projects_sources: Iterable[InternationalProjectsSource],
    international_projects_activity: ActivityMapping,
    person_stable_target_ids_by_query_string: dict[str, list[MergedPersonIdentifier]],
    unit_stable_target_id_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    funding_sources_stable_target_id_by_query: dict[str, MergedOrganizationIdentifier],
    partner_organizations_stable_target_id_by_query: dict[
        str, MergedOrganizationIdentifier
    ],
) -> list[ExtractedActivity]:
    """Transform international projects sources to extracted activity.

    Args:
        international_projects_sources: international projects sources
        international_projects_activity: activity mapping model with default values
        person_stable_target_ids_by_query_string: Mapping from author query
                                                  to person stable target ID
        unit_stable_target_id_by_synonym: Mapping from unit acronyms and labels
                                          to unit stable target ID
        funding_sources_stable_target_id_by_query: Mapping from funding sources
                                                   to organization stable target ID
        partner_organizations_stable_target_id_by_query: Mapping from partner orgs to
                                                         their stable target ID

    Returns:
        List of ExtractedActivity instances
    """
    return [
        activity
        for source in watch_progress(
            international_projects_sources,
            "transform_international_projects_sources_to_extracted_activities",
        )
        if (
            activity := transform_international_projects_source_to_extracted_activity(
                source,
                international_projects_activity,
                person_stable_target_ids_by_query_string,
                unit_stable_target_id_by_synonym,
                funding_sources_stable_target_id_by_query,
                partner_organizations_stable_target_id_by_query,
            )
        )
    ]


def get_theme_for_activity_or_topic(
    theme: list[MappingField[list[Theme]]],
    activity1: str | None,
    activity2: str | None,
    topic1: str | None,
    topic2: str | None,
) -> list[Theme]:
    """Get theme identifier for activities and topics.

    Args:
        theme: theme extracted from mapping
        activity1: activity 1 from the international-projects raw data file
        activity2: activity 2 from the international-projects raw data file
        topic1: topic 1 from the international-projects raw data file
        topic2: topic 2 from the international-projects raw data file

    Returns:
        Sorted list of Theme
    """
    themes_dict_from_mapping: dict[str, Theme] = {}
    default_theme_from_mapping = cast(
        "list[Theme]",
        theme[0].mappingRules[0].setValues,
    )[0]

    for theme_item in theme:
        for mapping_rule in theme_item.mappingRules:
            if mapping_rule.forValues and mapping_rule.setValues:
                themes_dict_from_mapping.update(
                    dict.fromkeys(mapping_rule.forValues, mapping_rule.setValues[0])
                )

    def get_theme_or_default(key: str | None) -> Theme:
        if key and (theme := themes_dict_from_mapping.get(key)):
            return theme
        return default_theme_from_mapping

    theme_set = set()
    theme_set.add(get_theme_or_default(activity1))
    theme_set.add(get_theme_or_default(activity2))
    theme_set.add(get_theme_or_default(topic1))
    theme_set.add(get_theme_or_default(topic2))

    return sorted(theme_set, key=lambda x: x.name)


def get_or_create_partner_organization(
    partner_organization: list[str],
    extracted_organizations: dict[str, MergedOrganizationIdentifier],
) -> list[MergedOrganizationIdentifier]:
    """Get partner organizations merged ids.

    Args:
        partner_organization: partner organizations from the source
        extracted_organizations: merged organization identifier extracted from wikidata

    Returns:
        list of matched or created merged organization identifier
    """
    final_partner_organizations: list[MergedOrganizationIdentifier] = []
    for partner_org in partner_organization:
        if wpo := extracted_organizations.get(partner_org):
            final_partner_organizations.append(wpo)
        elif partner_org not in ("None", "Not applicable"):
            extracted_organization = ExtractedOrganization(
                officialName=[Text(value=partner_org)],
                identifierInPrimarySource=partner_org,
                hadPrimarySource=get_extracted_primary_source_id_by_name(
                    "international-projects"
                ),
            )
            load([extracted_organization])
            final_partner_organizations.append(
                MergedOrganizationIdentifier(extracted_organization.stableTargetId)
            )
    return final_partner_organizations
