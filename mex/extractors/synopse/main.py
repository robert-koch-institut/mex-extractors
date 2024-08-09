from itertools import chain, groupby, tee

from mex.common.cli import entrypoint
from mex.common.ldap.extract import get_merged_ids_by_query_string
from mex.common.ldap.models.person import LDAPPersonWithQuery
from mex.common.ldap.transform import transform_ldap_persons_with_query_to_mex_persons
from mex.common.models import (
    ExtractedAccessPlatform,
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedOrganizationalUnit,
    ExtractedPrimarySource,
    ExtractedResource,
    ExtractedVariableGroup,
)
from mex.common.organigram.extract import (
    get_unit_merged_ids_by_emails,
)
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
    MergedResourceIdentifier,
)
from mex.extractors.mapping.extract import extract_mapping_data
from mex.extractors.pipeline import asset, run_job_in_process
from mex.extractors.settings import Settings
from mex.extractors.sinks import load
from mex.extractors.synopse.extract import (
    extract_projects,
    extract_study_data,
    extract_study_overviews,
    extract_synopse_organizations,
    extract_synopse_project_contributors,
    extract_variables,
)
from mex.extractors.synopse.filter import filter_and_log_access_platforms
from mex.extractors.synopse.models.project import SynopseProject
from mex.extractors.synopse.models.study import SynopseStudy
from mex.extractors.synopse.models.study_overview import SynopseStudyOverview
from mex.extractors.synopse.models.variable import SynopseVariable
from mex.extractors.synopse.transform import (
    split_off_extended_data_use_variables,
    transform_overviews_to_resource_lookup,
    transform_synopse_data_extended_data_use_to_mex_resources,
    transform_synopse_data_regular_to_mex_resources,
    transform_synopse_projects_to_mex_activities,
    transform_synopse_studies_into_access_platforms,
    transform_synopse_variables_to_mex_variable_groups,
    transform_synopse_variables_to_mex_variables,
)
from mex.extractors.wikidata.extract import (
    get_merged_organization_id_by_query_with_transform_and_load,
)


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
def synopse_variables() -> list[SynopseVariable]:
    """Extract variables from Synopse."""
    return list(extract_variables())


@asset(group_name="synopse")
def synopse_variables_extended_data_use_by_study_id(
    synopse_variables: list[SynopseVariable],
    synopse_study_overviews: list[SynopseStudyOverview],
) -> dict[int, list[SynopseVariable]]:
    """Convert Synopse data to synopse_variables_extended_data_use_by_study_id."""
    (
        _,
        variables_extended_data_use,
    ) = split_off_extended_data_use_variables(
        synopse_variables, synopse_study_overviews
    )

    sorted_variables = sorted(variables_extended_data_use, key=lambda v: v.studie_id)
    return {
        studie_id: list(variables)
        for studie_id, variables in groupby(sorted_variables, key=lambda v: v.studie_id)
    }


@asset(group_name="synopse")
def synopse_variables_regular_use_by_study_id(
    synopse_variables: list[SynopseVariable],
    synopse_study_overviews: list[SynopseStudyOverview],
) -> dict[int, list[SynopseVariable]]:
    """Convert Synopse data to synopse_variables_regular_use_by_study_id."""
    (
        variables_regular,
        _,
    ) = split_off_extended_data_use_variables(
        synopse_variables, synopse_study_overviews
    )
    sorted_variables = sorted(variables_regular, key=lambda v: v.studie_id)
    return {
        studie_id: list(variables)
        for studie_id, variables in groupby(sorted_variables, key=lambda v: v.studie_id)
    }


@asset(group_name="synopse")
def synopse_variables_regular_use_by_thema(
    synopse_variables: list[SynopseVariable],
    synopse_study_overviews: list[SynopseStudyOverview],
) -> dict[str, list[SynopseVariable]]:
    """Convert Synopse data to synopse_variables_regular_use_by_thema."""
    (
        variables_regular,
        _,
    ) = split_off_extended_data_use_variables(
        synopse_variables, synopse_study_overviews
    )

    sorted_variables = sorted(
        variables_regular, key=lambda v: v.thema_und_fragebogenausschnitt
    )
    return {
        thema: list(variables)
        for thema, variables in groupby(
            sorted_variables, key=lambda v: v.thema_und_fragebogenausschnitt
        )
    }


@asset(group_name="synopse")
def unit_stable_target_ids_by_emails(
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
) -> dict[str, MergedOrganizationalUnitIdentifier]:
    """Group organizational units by their email addresses."""
    return get_unit_merged_ids_by_emails(extracted_organizational_units)


@asset(group_name="synopse")
def extracted_synopse_contributor_stable_target_ids_by_name(
    synopse_project_contributors: list[LDAPPersonWithQuery],
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
    extracted_primary_source_ldap: ExtractedPrimarySource,
) -> dict[str, list[MergedPersonIdentifier]]:
    """Get lookup from contributor name to extracted person stable target id.

    Also transforms Synopse data to extracted persons
    """
    transformed_project_contributors = list(
        transform_ldap_persons_with_query_to_mex_persons(
            synopse_project_contributors,
            extracted_primary_source_ldap,
            extracted_organizational_units,
        )
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
    extracted_primary_source_wikidata: ExtractedPrimarySource,
    synopse_projects: list[SynopseProject],
) -> dict[str, MergedOrganizationIdentifier]:
    """Extract organizations for FF Projects from wikidata and group them by query."""
    wikidata_organizations_by_query = extract_synopse_organizations(synopse_projects)
    return get_merged_organization_id_by_query_with_transform_and_load(
        wikidata_organizations_by_query, extracted_primary_source_wikidata
    )


@asset(group_name="synopse")
def extracted_synopse_resource_stable_target_ids_by_synopse_id(
    synopse_projects: list[SynopseProject],
    synopse_studies: list[SynopseStudy],
    synopse_study_overviews: list[SynopseStudyOverview],
    synopse_variables_extended_data_use_by_study_id: dict[int, list[SynopseVariable]],
    synopse_variables_regular_use_by_study_id: dict[int, list[SynopseVariable]],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_synopse_access_platforms: list[ExtractedAccessPlatform],
    extracted_synopse_activities: list[ExtractedActivity],
    extracted_organization_rki: ExtractedOrganization,
    extracted_primary_source_report_server: ExtractedPrimarySource,
) -> dict[str, list[MergedResourceIdentifier]]:
    """Get lookup from synopse_id to extracted resource stable target id.

    Also transforms Synopse data to extracted resources
    """
    settings = Settings.get()
    synopse_resource_extended_data_use = extract_mapping_data(
        settings.synopse.mapping_path / "resource_extended-data-use.yaml",
        ExtractedResource,
    )
    transformed_study_data_regular_resources = (
        transform_synopse_data_regular_to_mex_resources(
            synopse_studies,
            synopse_projects,
            synopse_variables_regular_use_by_study_id,
            extracted_synopse_activities,
            extracted_synopse_access_platforms,
            extracted_primary_source_report_server,
            unit_stable_target_ids_by_synonym,
            extracted_organization_rki,
            synopse_resource_extended_data_use,
        )
    )
    transformed_study_data_regular_resource_gens = tee(
        transformed_study_data_regular_resources, 2
    )
    load(transformed_study_data_regular_resource_gens[0])
    settings = Settings.get()
    synopse_resource = extract_mapping_data(
        settings.synopse.mapping_path / "resource.yaml",
        ExtractedResource,
    )
    transformed_study_data_resources_extended_data_use = (
        transform_synopse_data_extended_data_use_to_mex_resources(
            synopse_studies,
            synopse_projects,
            synopse_variables_extended_data_use_by_study_id,
            extracted_synopse_activities,
            extracted_synopse_access_platforms,
            extracted_primary_source_report_server,
            unit_stable_target_ids_by_synonym,
            extracted_organization_rki,
            synopse_resource,
        )
    )
    transformed_study_data_resource_extended_data_use_gens = tee(
        transformed_study_data_resources_extended_data_use, 2
    )
    load(transformed_study_data_resource_extended_data_use_gens[0])

    return transform_overviews_to_resource_lookup(
        synopse_study_overviews,
        chain(
            transformed_study_data_regular_resource_gens[1],
            transformed_study_data_resource_extended_data_use_gens[1],
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
    synopse_access_platform = extract_mapping_data(
        settings.synopse.mapping_path / "access-platform.yaml",
        ExtractedAccessPlatform,
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
def extracted_synopse_activities(
    synopse_projects: list[SynopseProject],
    extracted_primary_source_report_server: ExtractedPrimarySource,
    unit_stable_target_ids_by_emails: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_synopse_contributor_stable_target_ids_by_name: dict[
        str, list[MergedPersonIdentifier]
    ],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    synopse_organization_ids_by_query_string: dict[str, MergedOrganizationIdentifier],
) -> list[ExtractedActivity]:
    """Transforms Synopse data to extracted activities and load result."""
    settings = Settings.get()
    synopse_activity = extract_mapping_data(
        settings.synopse.mapping_path / "activity.yaml",
        ExtractedActivity,
    )
    transformed_activities = list(
        transform_synopse_projects_to_mex_activities(
            synopse_projects,
            extracted_primary_source_report_server,
            unit_stable_target_ids_by_emails,
            extracted_synopse_contributor_stable_target_ids_by_name,
            unit_stable_target_ids_by_synonym,
            synopse_activity,
            synopse_organization_ids_by_query_string,
        )
    )
    load(transformed_activities)
    return transformed_activities


@asset(group_name="synopse")
def extracted_synopse_variable_groups(
    synopse_variables_regular_use_by_thema: dict[str, list[SynopseVariable]],
    extracted_primary_source_report_server: ExtractedPrimarySource,
    extracted_synopse_resource_stable_target_ids_by_synopse_id: dict[
        str, list[MergedResourceIdentifier]
    ],
) -> list[ExtractedVariableGroup]:
    """Transforms Synopse data to extracted variable groups and load result."""
    transformed_variable_groups = list(
        transform_synopse_variables_to_mex_variable_groups(
            synopse_variables_regular_use_by_thema,
            extracted_primary_source_report_server,
            extracted_synopse_resource_stable_target_ids_by_synopse_id,
        )
    )
    load(transformed_variable_groups)
    return transformed_variable_groups


@asset(group_name="synopse")
def extracted_synopse_variables(
    synopse_variables_regular_use_by_thema: dict[str, list[SynopseVariable]],
    extracted_primary_source_report_server: ExtractedPrimarySource,
    extracted_synopse_variable_groups: list[ExtractedVariableGroup],
    extracted_synopse_resource_stable_target_ids_by_synopse_id: dict[
        str, list[MergedResourceIdentifier]
    ],
) -> None:
    """Transforms Synopse data to extracted variables and load result."""
    extracted_variables = transform_synopse_variables_to_mex_variables(
        synopse_variables_regular_use_by_thema,
        extracted_synopse_variable_groups,
        extracted_synopse_resource_stable_target_ids_by_synopse_id,
        extracted_primary_source_report_server,
    )
    load(extracted_variables)


@entrypoint(Settings)
def run() -> None:
    """Run the synopse extractor job in-process."""
    run_job_in_process("synopse")
