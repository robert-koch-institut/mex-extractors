from collections.abc import Mapping
from itertools import chain, groupby, tee
from typing import Any

from dagster import asset

from mex.common.cli import entrypoint
from mex.common.ldap.extract import get_merged_ids_by_query_string
from mex.common.ldap.models import LDAPPersonWithQuery
from mex.common.ldap.transform import (
    transform_ldap_actors_to_mex_contact_points,
    transform_ldap_persons_with_query_to_mex_persons,
)
from mex.common.models import (
    AccessPlatformMapping,
    ActivityMapping,
    ExtractedAccessPlatform,
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedOrganizationalUnit,
    ExtractedPrimarySource,
    ExtractedVariableGroup,
    ResourceMapping,
)
from mex.common.types import (
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
    MergedResourceIdentifier,
)
from mex.extractors.pipeline import run_job_in_process
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
    filter_and_log_access_platforms,
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
    return list(extract_projects())


@asset(group_name="synopse")
def synopse_project_contributors(
    synopse_projects: list[SynopseProject],
) -> list[LDAPPersonWithQuery]:
    """Extract project contributors from Synopse."""
    return list(extract_synopse_project_contributors(synopse_projects))


@asset(group_name="synopse")
def synopse_studies() -> list[SynopseStudy]:
    """Extract studies from Synopse."""
    return list(extract_study_data())


@asset(group_name="synopse")
def synopse_study_overviews() -> list[SynopseStudyOverview]:
    """Extract study overviews from Synopse."""
    return list(extract_study_overviews())


@asset(group_name="synopse")
def synopse_variables(
    extracted_primary_source_report_server: ExtractedPrimarySource,
) -> list[SynopseVariable]:
    """Extract variables from Synopse."""
    return filter_and_log_synopse_variables(
        extract_variables(), extracted_primary_source_report_server
    )


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
def extracted_synopse_contributor_stable_target_ids_by_name(
    synopse_project_contributors: list[LDAPPersonWithQuery],
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
    extracted_primary_source_ldap: ExtractedPrimarySource,
) -> dict[str, list[MergedPersonIdentifier]]:
    """Get lookup from contributor name to extracted person stable target id.

    Also transforms Synopse data to extracted persons
    """
    transformed_project_contributors = transform_ldap_persons_with_query_to_mex_persons(
        synopse_project_contributors,
        extracted_primary_source_ldap,
        extracted_organizational_units,
    )
    load(transformed_project_contributors)
    return get_merged_ids_by_query_string(  # only works after contributors are loaded
        # reason: backend is queried for identities of contributors, contributors not
        # in backend are skipped
        synopse_project_contributors,
        extracted_primary_source_ldap,
    )


@asset(group_name="synopse")
def synopse_organization_ids_by_query_string(
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
def contact_merged_id_by_query_string(
    synopse_activity: dict[str, Any],
    synopse_resource: dict[str, Any],
    extracted_primary_source_ldap: ExtractedPrimarySource,
) -> dict[str, MergedContactPointIdentifier]:
    """Get lookup of ldap functional accounts by email."""
    synopse_contact = extract_synopse_contact(
        ResourceMapping.model_validate(synopse_resource),
        ActivityMapping.model_validate(synopse_activity),
    )
    contact_points = transform_ldap_actors_to_mex_contact_points(
        synopse_contact,
        extracted_primary_source_ldap,
    )
    load(contact_points)
    return {
        contact_point.email[0].lower(): contact_point.stableTargetId
        for contact_point in contact_points
    }


@asset(group_name="synopse")
def extracted_synopse_resource_stable_target_ids_by_synopse_id(  # noqa: PLR0913
    synopse_projects: list[SynopseProject],
    synopse_studies: list[SynopseStudy],
    synopse_study_overviews: list[SynopseStudyOverview],
    synopse_variables_by_study_id: dict[int, list[SynopseVariable]],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_synopse_access_platforms: list[ExtractedAccessPlatform],
    extracted_synopse_activities: list[ExtractedActivity],
    extracted_organization_rki: ExtractedOrganization,
    extracted_primary_source_report_server: ExtractedPrimarySource,
    synopse_resource: dict[str, Any],
    contact_merged_id_by_query_string: dict[str, MergedContactPointIdentifier],
) -> dict[str, list[MergedResourceIdentifier]]:
    """Get lookup from synopse_id to extracted resource stable target id.

    Also transforms Synopse data to extracted resources
    """
    transformed_study_data_resources = transform_synopse_data_to_mex_resources(
        synopse_studies,
        synopse_projects,
        synopse_variables_by_study_id,
        extracted_synopse_activities,
        extracted_synopse_access_platforms,
        extracted_primary_source_report_server,
        unit_stable_target_ids_by_synonym,
        extracted_organization_rki,
        ResourceMapping.model_validate(synopse_resource),
        contact_merged_id_by_query_string,
    )
    transformed_study_data_resource_gens = tee(transformed_study_data_resources, 2)
    load(transformed_study_data_resource_gens[0])
    return transform_overviews_to_resource_lookup(
        synopse_study_overviews,
        chain(
            transformed_study_data_resource_gens[1],
        ),
    )


@asset(group_name="synopse")
def extracted_synopse_access_platforms(
    synopse_studies: list[SynopseStudy],
    extracted_primary_source_report_server: ExtractedPrimarySource,
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> list[ExtractedAccessPlatform]:
    """Transform Synopse data to extracted access platforms and load result."""
    settings = Settings.get()
    synopse_access_platform = AccessPlatformMapping.model_validate(
        load_yaml(settings.synopse.mapping_path / "access-platform.yaml"),
    )
    synopse_studies_filtered = filter_and_log_access_platforms(
        synopse_studies, extracted_primary_source_report_server
    )
    transformed_access_platforms = list(
        transform_synopse_studies_into_access_platforms(
            synopse_studies_filtered,
            unit_stable_target_ids_by_synonym,
            extracted_primary_source_report_server,
            synopse_access_platform,
        )
    )
    load(transformed_access_platforms)
    return transformed_access_platforms


@asset(group_name="synopse")
def synopse_activity() -> dict[str, Any]:
    """Extract and transform synopse activity default values."""
    settings = Settings.get()
    return load_yaml(settings.synopse.mapping_path / "activity.yaml")


@asset(group_name="synopse")
def extracted_synopse_activities(  # noqa: PLR0913
    synopse_projects: list[SynopseProject],
    extracted_primary_source_report_server: ExtractedPrimarySource,
    extracted_synopse_contributor_stable_target_ids_by_name: dict[
        str, list[MergedPersonIdentifier]
    ],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    synopse_organization_ids_by_query_string: dict[str, MergedOrganizationIdentifier],
    synopse_activity: dict[str, Any],
    contact_merged_id_by_query_string: Mapping[
        str,
        MergedOrganizationalUnitIdentifier
        | MergedPersonIdentifier
        | MergedContactPointIdentifier,
    ],
) -> list[ExtractedActivity]:
    """Transforms Synopse data to extracted activities and load result."""
    non_child_activities, child_activities = (
        transform_synopse_projects_to_mex_activities(
            synopse_projects,
            extracted_primary_source_report_server,
            extracted_synopse_contributor_stable_target_ids_by_name,
            unit_stable_target_ids_by_synonym,
            ActivityMapping.model_validate(synopse_activity),
            synopse_organization_ids_by_query_string,
            contact_merged_id_by_query_string,
        )
    )

    load(non_child_activities)
    load(child_activities)
    return [*non_child_activities, *child_activities]


@asset(group_name="synopse")
def extracted_synopse_variable_groups(
    synopse_variables_by_thema: dict[str, list[SynopseVariable]],
    extracted_primary_source_report_server: ExtractedPrimarySource,
    extracted_synopse_resource_stable_target_ids_by_synopse_id: dict[
        str, list[MergedResourceIdentifier]
    ],
) -> list[ExtractedVariableGroup]:
    """Transforms Synopse data to extracted variable groups and load result."""
    transformed_variable_groups = list(
        transform_synopse_variables_to_mex_variable_groups(
            synopse_variables_by_thema,
            extracted_primary_source_report_server,
            extracted_synopse_resource_stable_target_ids_by_synopse_id,
        )
    )
    load(transformed_variable_groups)
    return transformed_variable_groups


@asset(group_name="synopse")
def extracted_synopse_variables(
    synopse_variables_by_thema: dict[str, list[SynopseVariable]],
    extracted_primary_source_report_server: ExtractedPrimarySource,
    extracted_synopse_variable_groups: list[ExtractedVariableGroup],
    extracted_synopse_resource_stable_target_ids_by_synopse_id: dict[
        str, list[MergedResourceIdentifier]
    ],
) -> None:
    """Transforms Synopse data to extracted variables and load result."""
    extracted_variables = transform_synopse_variables_to_mex_variables(
        synopse_variables_by_thema,
        extracted_synopse_variable_groups,
        extracted_synopse_resource_stable_target_ids_by_synopse_id,
        extracted_primary_source_report_server,
    )
    load(extracted_variables)


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the synopse extractor job in-process."""
    run_job_in_process("synopse")
