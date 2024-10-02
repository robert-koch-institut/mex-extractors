from collections.abc import Generator, Hashable, Iterable
from typing import Any

from mex.common.logging import watch
from mex.common.models import (
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedPrimarySource,
)
from mex.common.types import Identifier, MergedOrganizationIdentifier
from mex.extractors.blueant.models.source import BlueAntSource
from mex.extractors.sinks import load


@watch
def transform_blueant_sources_to_extracted_activities(
    blueant_sources: Iterable[BlueAntSource],
    primary_source: ExtractedPrimarySource,
    person_stable_target_ids_by_employee_id: dict[Hashable, list[Identifier]],
    unit_stable_target_ids_by_synonym: dict[str, Identifier],
    activity: dict[str, Any],
    blueant_organization_ids_by_query_string: dict[str, MergedOrganizationIdentifier],
) -> Generator[ExtractedActivity, None, None]:
    """Transform Blue Ant sources to ExtractedActivities.

    Args:
        blueant_sources: Blue Ant sources
        primary_source: MEx primary_source for Blue Ant
        person_stable_target_ids_by_employee_id: Mapping from LDAP employeeIDs
                                                 to person stable target IDs
        unit_stable_target_ids_by_synonym: Map from unit acronyms and labels
                                           to unit stable target IDs
        activity: activity mapping default values
        blueant_organization_ids_by_query_string: extracted blueant organizations dict

    Returns:
        Generator for ExtractedActivity instances
    """
    activity_type_values_by_type_id = {
        for_value: mapping_rule["setValues"]
        for mapping_rule in activity["activityType"][0]["mappingRules"]
        if mapping_rule["forValues"]
        for for_value in mapping_rule["forValues"]
    }
    for source in blueant_sources:
        # find source type
        activity_type = (
            [activity_type_values_by_type_id[source.type_]]
            if source.type_ in activity_type_values_by_type_id.keys()
            else []
        )
        funder_or_commissioner: list[MergedOrganizationIdentifier] = []
        for name in source.client_names:
            if name in blueant_organization_ids_by_query_string.keys():
                funder_or_commissioner.append(
                    blueant_organization_ids_by_query_string[name]
                )
            elif name != "Robert Koch-Institut":
                extracted_organization = ExtractedOrganization(
                    officialName=name,
                    identifierInPrimarySource=name,
                    hadPrimarySource=primary_source.stableTargetId,
                )
                load([extracted_organization])
                funder_or_commissioner.append(
                    MergedOrganizationIdentifier(extracted_organization.stableTargetId)
                )

        # find responsible unit
        department = source.department.replace("(h)", "").strip()
        if department in unit_stable_target_ids_by_synonym.keys():
            responsible_unit = unit_stable_target_ids_by_synonym.get(department)
        else:
            continue

        # get contact employee or fallback to unit
        contact = person_stable_target_ids_by_employee_id[
            source.projectLeaderEmployeeId
        ]
        if not contact and responsible_unit:
            contact.append(responsible_unit)

        yield ExtractedActivity(
            start=source.start,
            end=source.end,
            activityType=activity_type,
            contact=contact,
            involvedPerson=person_stable_target_ids_by_employee_id[
                source.projectLeaderEmployeeId
            ],
            hadPrimarySource=primary_source.stableTargetId,
            responsibleUnit=responsible_unit,
            funderOrCommissioner=funder_or_commissioner,
            title=source.name,
            identifierInPrimarySource=source.number,
        )
