from itertools import groupby
from typing import Any

from dagster import asset

from mex.common.cli import entrypoint
from mex.common.ldap.extract import get_merged_ids_by_query_string
from mex.common.ldap.models import LDAPPersonWithQuery
from mex.common.ldap.transform import (
    transform_ldap_functional_accounts_to_extracted_contact_points,
    transform_ldap_persons_with_query_to_extracted_persons,
)
from mex.common.models import (
    AccessPlatformMapping,
    ActivityMapping,
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedOrganizationalUnit,
    ExtractedResource,
    ExtractedVariable,
    ExtractedVariableGroup,
    ResourceMapping,
)
from mex.common.types import (
    MergedAccessPlatformIdentifier,
    MergedContactPointIdentifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
)
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.settings import Settings
from mex.extractors.sinks import load
from mex.extractors.synopse.extract import (
    extract_projects,
    extract_study_data,
    extract_study_overviews,
    extract_synopse_contact,
    extract_synopse_organizations,
    extract_synopse_project_contributors,
    extract_variables,
)
from mex.extractors.synopse.filter import (
    filter_and_log_synopse_variables,
)
from mex.extractors.synopse.models.project import SynopseProject
from mex.extractors.synopse.models.study import SynopseStudy
from mex.extractors.synopse.models.study_overview import SynopseStudyOverview
from mex.extractors.synopse.models.variable import SynopseVariable
from mex.extractors.synopse.transform import (
    transform_overviews_to_resource_lookup,
    transform_synopse_data_to_mex_resources,
    transform_synopse_projects_to_mex_activities,
    transform_synopse_studies_into_access_platforms,
    transform_synopse_variables_to_mex_variable_groups,
    transform_synopse_variables_to_mex_variables,
)
from mex.extractors.utils import load_yaml


@asset(group_name="synopse")
def synopse_projects() -> list[SynopseProject]:
    """Extract projects from Synopse."""
    return extract_projects()


@asset(group_name="synopse")
def synopse_ldap_persons_with_query(
    synopse_projects: list[SynopseProject],
) -> list[LDAPPersonWithQuery]:
    """Extract project contributors from Synopse."""
    return extract_synopse_project_contributors(synopse_projects)


@asset(group_name="synopse")
def synopse_studies() -> list[SynopseStudy]:
    """Extract studies from Synopse."""
    return extract_study_data()


@asset(group_name="synopse")
def synopse_study_overviews() -> list[SynopseStudyOverview]:
    """Extract study overviews from Synopse."""
    return extract_study_overviews()


@asset(group_name="synopse")
def synopse_variables() -> list[SynopseVariable]:
    """Extract variables from Synopse."""
    return filter_and_log_synopse_variables(extract_variables())


@asset(group_name="synopse")
def synopse_variables_by_study_id(
    synopse_variables: list[SynopseVariable],
) -> dict[int, list[SynopseVariable]]:
    """Convert Synopse data to synopse_variables_by_study_id."""
    sorted_variables = sorted(synopse_variables, key=lambda v: v.studie_id)
    return {
        studie_id: list(variables)
        for studie_id, variables in groupby(sorted_variables, key=lambda v: v.studie_id)
    }


@asset(group_name="synopse")
def synopse_variables_by_thema(
    synopse_variables: list[SynopseVariable],
) -> dict[str, list[SynopseVariable]]:
    """Convert Synopse data to synopse_variables_by_thema."""
    sorted_variables = sorted(
        synopse_variables, key=lambda v: v.thema_und_fragebogenausschnitt
    )
    return {
        thema: list(variables)
        for thema, variables in groupby(
            sorted_variables, key=lambda v: v.thema_und_fragebogenausschnitt
        )
    }


@asset(group_name="synopse")
def synopse_merged_person_ids_by_name_str(
    synopse_ldap_persons_with_query: list[LDAPPersonWithQuery],
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
    extracted_organization_rki: ExtractedOrganization,
) -> dict[str, list[MergedPersonIdentifier]]:
    """Get lookup from contributor name to extracted person stable target id.

    Also transforms Synopse data to extracted persons
    """
    transformed_project_contributors = (
        transform_ldap_persons_with_query_to_extracted_persons(
            synopse_ldap_persons_with_query,
            get_extracted_primary_source_id_by_name("ldap"),
            extracted_organizational_units,
            extracted_organization_rki,
        )
    )
    load(transformed_project_contributors)
    return get_merged_ids_by_query_string(  # only works after contributors are loaded
        # reason: backend is queried for identities of contributors, contributors not
        # in backend are skipped
        synopse_ldap_persons_with_query,
        get_extracted_primary_source_id_by_name("ldap"),
    )


@asset(group_name="synopse")
def synopse_merged_organization_ids_by_query_string(
    synopse_projects: list[SynopseProject],
) -> dict[str, MergedOrganizationIdentifier]:
    """Extract organizations for FF Projects from wikidata and group them by query."""
    return extract_synopse_organizations(synopse_projects)


@asset(group_name="synopse")
def synopse_resource() -> dict[str, Any]:
    """Extract and transform synopse resource default values."""
    settings = Settings.get()
    return load_yaml(settings.synopse.mapping_path / "resource.yaml")


@asset(group_name="synopse")
def synopse_merged_contact_point_ids_by_query_string() -> dict[
    str, MergedContactPointIdentifier
]:
    """Get lookup of ldap functional accounts by email."""
    settings = Settings.get()
    synopse_access_platform = AccessPlatformMapping.model_validate(
        load_yaml(settings.synopse.mapping_path / "access-platform.yaml"),
    )

    synopse_contact = extract_synopse_contact(synopse_access_platform)
    contact_points = transform_ldap_functional_accounts_to_extracted_contact_points(
        synopse_contact, get_extracted_primary_source_id_by_name("ldap")
    )
    load(contact_points)

    return {
        contact_point.email[0].lower(): contact_point.stableTargetId
        for contact_point in contact_points
    }


@asset(group_name="synopse")
def synopse_access_platform_id(
    synopse_merged_contact_point_ids_by_query_string: dict[
        str, MergedContactPointIdentifier
    ],
) -> MergedAccessPlatformIdentifier:
    """Transform Synopse data to extracted access platforms and load result."""
    settings = Settings.get()
    synopse_access_platform = AccessPlatformMapping.model_validate(
        load_yaml(settings.synopse.mapping_path / "access-platform.yaml"),
    )

    transformed_access_platforms = transform_synopse_studies_into_access_platforms(
        synopse_merged_contact_point_ids_by_query_string,
        synopse_access_platform,
    )

    load([transformed_access_platforms])
    return transformed_access_platforms.stableTargetId


@asset(group_name="synopse")
def synopse_extracted_resources_by_identifier_in_primary_source(  # noqa: PLR0913
    synopse_projects: list[SynopseProject],
    synopse_studies: list[SynopseStudy],
    synopse_study_overviews: list[SynopseStudyOverview],
    synopse_variables_by_study_id: dict[int, list[SynopseVariable]],
    synopse_extracted_activities: list[ExtractedActivity],
    extracted_organization_rki: ExtractedOrganization,
    synopse_resource: dict[str, Any],
    synopse_access_platform_id: MergedAccessPlatformIdentifier,
    synopse_merged_person_ids_by_name_str: dict[str, list[MergedPersonIdentifier]],
) -> dict[str, ExtractedResource]:
    """Get lookup from synopse_id to extracted resource identifier in primary source.

    Also transforms Synopse data to extracted resources
    """
    transformed_study_data_resources = transform_synopse_data_to_mex_resources(
        synopse_studies,
        synopse_projects,
        synopse_variables_by_study_id,
        synopse_extracted_activities,
        extracted_organization_rki,
        ResourceMapping.model_validate(synopse_resource),
        synopse_access_platform_id,
        synopse_merged_person_ids_by_name_str,
    )
    load(transformed_study_data_resources)
    return transform_overviews_to_resource_lookup(
        synopse_study_overviews,
        transformed_study_data_resources,
    )


@asset(group_name="synopse")
def synopse_activity() -> dict[str, Any]:
    """Extract and transform synopse activity default values."""
    settings = Settings.get()
    return load_yaml(settings.synopse.mapping_path / "activity.yaml")


@asset(group_name="synopse")
def synopse_extracted_activities(
    synopse_projects: list[SynopseProject],
    synopse_merged_person_ids_by_name_str: dict[str, list[MergedPersonIdentifier]],
    synopse_merged_organization_ids_by_query_string: dict[
        str, MergedOrganizationIdentifier
    ],
    synopse_activity: dict[str, Any],
) -> list[ExtractedActivity]:
    """Transforms Synopse data to extracted activities and load result."""
    non_child_activities, child_activities = (
        transform_synopse_projects_to_mex_activities(
            synopse_projects,
            synopse_merged_person_ids_by_name_str,
            ActivityMapping.model_validate(synopse_activity),
            synopse_merged_organization_ids_by_query_string,
        )
    )

    load(non_child_activities)
    load(child_activities)
    return [*non_child_activities, *child_activities]


@asset(group_name="synopse")
def synopse_variable_groups_by_identifier_in_primary_source(
    synopse_variables_by_thema: dict[str, list[SynopseVariable]],
    synopse_extracted_resources_by_identifier_in_primary_source: dict[
        str, ExtractedResource
    ],
    synopse_study_overviews: list[SynopseStudyOverview],
) -> dict[str, ExtractedVariableGroup]:
    """Transforms Synopse data to extracted variable groups and load result."""
    transformed_variable_groups = transform_synopse_variables_to_mex_variable_groups(
        synopse_variables_by_thema,
        synopse_extracted_resources_by_identifier_in_primary_source,
        synopse_study_overviews,
    )
    load(transformed_variable_groups)
    return {vg.identifierInPrimarySource: vg for vg in transformed_variable_groups}


@asset(group_name="synopse")
def synopse_extracted_variables(
    synopse_variables_by_thema: dict[str, list[SynopseVariable]],
    synopse_variable_groups_by_identifier_in_primary_source: dict[
        str, ExtractedVariableGroup
    ],
    synopse_extracted_resources_by_identifier_in_primary_source: dict[
        str, ExtractedResource
    ],
    synopse_study_overviews: list[SynopseStudyOverview],
) -> list[ExtractedVariable]:
    """Transforms Synopse data to extracted variables and load result."""
    extracted_variables = transform_synopse_variables_to_mex_variables(
        synopse_variables_by_thema,
        synopse_variable_groups_by_identifier_in_primary_source,
        synopse_extracted_resources_by_identifier_in_primary_source,
        synopse_study_overviews,
    )
    load(extracted_variables)
    return extracted_variables


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the synopse extractor job in-process."""
    run_job_in_process("synopse")
