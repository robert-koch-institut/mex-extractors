from collections.abc import Generator, Hashable, Iterable
from typing import Any

from mex.common.logging import watch
from mex.common.models import (
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedPrimarySource,
)
from mex.common.types import (
    ActivityType,
    Link,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
    Text,
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
            if wfc := partner_organizations_stable_target_id_by_query.get(partner_org):
                all_partner_organizations.append(wfc)
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

    activity_types = international_projects_activity["activityType"][0]["mappingRules"]
    activity_types_dict: dict[str, ActivityType] = {}
    for activity_type in activity_types:
        for_value = activity_type.get("forValues")
        set_value = activity_type.get("setValues")[0]
        if not for_value:
            activity_types_dict.setdefault("Other", set_value)
            continue
        activity_types_dict.setdefault(for_value[0], set_value)

    return ExtractedActivity(
        title=source.full_project_name,
        activityType=(
            [activity_types_dict[source.funding_type]] if source.funding_type else []
        ),
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
        theme=[],
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
