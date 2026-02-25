import re
from typing import TYPE_CHECKING

from mex.common.models import (
    ActivityMapping,
    ExtractedActivity,
    ExtractedOrganization,
)
from mex.extractors.logging import watch_progress
from mex.extractors.organigram.helpers import get_unit_merged_id_by_synonym
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.sinks import load
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
)

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from mex.common.models.activity import AnyContactIdentifier
    from mex.common.types import (
        MergedOrganizationIdentifier,
        MergedPersonIdentifier,
    )
    from mex.extractors.blueant.models.source import BlueAntSource


def transform_blueant_sources_to_extracted_activities(
    blueant_sources: Iterable[BlueAntSource],
    person_stable_target_ids_by_employee_id: dict[str, list[MergedPersonIdentifier]],
    activity: ActivityMapping,
) -> list[ExtractedActivity]:
    """Transform Blue Ant sources to ExtractedActivities.

    Args:
        blueant_sources: Blue Ant sources
        person_stable_target_ids_by_employee_id: Mapping from LDAP employeeIDs
                                                 to person stable target IDs
        activity: activity mapping model with default values

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
    seen_merged_org_ids_by_query_str: dict[str, MergedOrganizationIdentifier] = {}
    for source in watch_progress(
        blueant_sources, "transform_blueant_sources_to_extracted_activities"
    ):
        # find source type
        activity_type = activity_type_values_by_type_id.get(source.type_, [])
        funder_or_commissioner: list[MergedOrganizationIdentifier] = []
        for name in source.client_names:
            if not name or name in {"Robert Koch-Institut", "RKI"}:
                continue

            # Check if organization "name" was already used and can be reused. If not,
            # check if that organization can be found with wikidata. If not,
            # create and load a new ExtractedOrganization
            if (org_id := seen_merged_org_ids_by_query_str.get(name)) is None:
                if (
                    org_id := get_wikidata_extracted_organization_id_by_name(name)
                ) is None:
                    extracted_organization = ExtractedOrganization(
                        officialName=name,
                        identifierInPrimarySource=name,
                        hadPrimarySource=get_extracted_primary_source_id_by_name(
                            "blueant"
                        ),
                    )
                    load([extracted_organization])
                    org_id = extracted_organization.stableTargetId
                seen_merged_org_ids_by_query_str[name] = org_id
            funder_or_commissioner.append(org_id)

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
