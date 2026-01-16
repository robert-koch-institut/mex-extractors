import re
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING

from mex.common.models import (
    ActivityMapping,
    ExtractedActivity,
    ExtractedOrganization,
)
from mex.common.types import (
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
)
from mex.extractors.blueant.models.source import BlueAntSource
from mex.extractors.logging import watch_progress
from mex.extractors.organigram.helpers import get_unit_merged_id_by_synonym
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.sinks import load

if TYPE_CHECKING:
    from mex.common.models.activity import AnyContactIdentifier


def transform_blueant_sources_to_extracted_activities(
    blueant_sources: Iterable[BlueAntSource],
    person_stable_target_ids_by_employee_id: dict[str, list[MergedPersonIdentifier]],
    activity: ActivityMapping,
    blueant_merged_organization_ids_by_query_string: dict[
        str, MergedOrganizationIdentifier
    ],
) -> list[ExtractedActivity]:
    """Transform Blue Ant sources to ExtractedActivities.

    Args:
        blueant_sources: Blue Ant sources
        person_stable_target_ids_by_employee_id: Mapping from LDAP employeeIDs
                                                 to person stable target IDs
        activity: activity mapping model with default values
        blueant_merged_organization_ids_by_query_string: extracted blueant organizations
                                                         dict

    Returns:
        List of ExtractedActivity instances
    """
    activity_type_values_by_type_id = {
        for_value: mapping_rule.setValues
        for mapping_rule in activity.activityType[0].mappingRules
        if mapping_rule.forValues
        for for_value in mapping_rule.forValues
    }
    activities = []
    for source in watch_progress(
        blueant_sources, "transform_blueant_sources_to_extracted_activities"
    ):
        # find source type
        activity_type = activity_type_values_by_type_id.get(source.type_, [])
        funder_or_commissioner: list[MergedOrganizationIdentifier] = []
        for name in source.client_names:
            if name:
                if name in blueant_merged_organization_ids_by_query_string:
                    funder_or_commissioner.append(
                        blueant_merged_organization_ids_by_query_string[name]
                    )
                elif name not in ["Robert Koch-Institut", "RKI"]:
                    extracted_organization = ExtractedOrganization(
                        officialName=name,
                        identifierInPrimarySource=name,
                        hadPrimarySource=get_extracted_primary_source_id_by_name(
                            "blueant"
                        ),
                    )
                    load([extracted_organization])
                    funder_or_commissioner.append(
                        MergedOrganizationIdentifier(
                            extracted_organization.stableTargetId
                        )
                    )
                    blueant_merged_organization_ids_by_query_string[name] = (
                        extracted_organization.stableTargetId
                    )

        # find responsible unit
        department = source.department.replace("(h)", "").strip()
        department_ids = get_unit_merged_id_by_synonym(department)
        if not department_ids:
            continue

        # get contact employee or fallback to unit
        contact: Sequence[AnyContactIdentifier]
        if ple_id := source.projectLeaderEmployeeId:
            contact = person_stable_target_ids_by_employee_id[ple_id]
        if not contact and department_ids:
            contact = department_ids

        source_name = re.sub(
            r"[\d*_]+|[FG\d* ]+[- ]+", "", source.name
        )  # strip according to mapping
        if source_name not in (activity.title[0].mappingRules[1].forValues or []):
            title = [source_name]
        else:
            title = []

        activities.append(
            ExtractedActivity(
                start=source.start,
                activityType=activity_type,
                contact=contact,
                involvedPerson=person_stable_target_ids_by_employee_id.get(
                    source.projectLeaderEmployeeId  # type: ignore[arg-type]
                ),
                hadPrimarySource=get_extracted_primary_source_id_by_name("blueant"),
                responsibleUnit=department_ids,
                funderOrCommissioner=funder_or_commissioner,
                title=title or [],
                identifierInPrimarySource=source.number,
            )
        )
    return activities
