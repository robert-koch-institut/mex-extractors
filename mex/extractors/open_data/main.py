from dagster import asset

from mex.common.cli import entrypoint
from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.transform import (
    transform_ldap_functional_account_to_extracted_contact_point,
)
from mex.common.models import (
    DistributionMapping,
    ExtractedContactPoint,
    ExtractedDistribution,
    ExtractedOrganization,
    ExtractedOrganizationalUnit,
    ExtractedPerson,
    ExtractedResource,
    ExtractedVariable,
    ExtractedVariableGroup,
    ResourceMapping,
)
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedResourceIdentifier,
)
from mex.extractors.open_data.extract import (
    extract_open_data_persons_from_open_data_parent_resources,
    extract_parent_resources,
    extract_tableschema,
)
from mex.extractors.open_data.models.source import (
    OpenDataCreatorsOrContributors,
    OpenDataParentResource,
    OpenDataTableSchema,
)
from mex.extractors.open_data.transform import (
    transform_open_data_distributions,
    transform_open_data_parent_resource_to_mex_resource,
    transform_open_data_person_affiliations_to_organizations,
    transform_open_data_persons,
    transform_open_data_variable_groups,
    transform_open_data_variables,
)
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.settings import Settings
from mex.extractors.sinks import load
from mex.extractors.utils import load_yaml

OPEN_DATA_EMAIL = "opendata@rki.de"


@asset(group_name="open_data")
def open_data_parent_resources() -> list[OpenDataParentResource]:
    """Extract open data parent resources from Zenodo."""
    return extract_parent_resources()


@asset(group_name="open_data")
def open_data_creators_contributors(
    open_data_parent_resources: list[OpenDataParentResource],
) -> list[OpenDataCreatorsOrContributors]:
    """Return unique open Data persons from open data parent resources."""
    return extract_open_data_persons_from_open_data_parent_resources(
        open_data_parent_resources
    )


@asset(group_name="open_data")
def open_data_organization_ids_by_name_str(
    open_data_creators_contributors: list[OpenDataCreatorsOrContributors],
) -> dict[str, MergedOrganizationIdentifier]:
    """Transform affiliations of open data persons to extracted organizations."""
    return transform_open_data_person_affiliations_to_organizations(
        open_data_creators_contributors
    )


@asset(group_name="open_data")
def open_data_extracted_persons(
    open_data_creators_contributors: list[OpenDataCreatorsOrContributors],
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
    extracted_organization_rki: ExtractedOrganization,
    open_data_organization_ids_by_name_str: dict[str, MergedOrganizationIdentifier],
) -> list[ExtractedPerson]:
    """Get Extracted persons and load them to sinks."""
    open_data_persons = transform_open_data_persons(
        open_data_creators_contributors,
        extracted_organizational_units,
        extracted_organization_rki,
        open_data_organization_ids_by_name_str,
    )
    load(open_data_persons)
    return open_data_persons


@asset(group_name="open_data")
def open_data_extracted_contact_points() -> list[ExtractedContactPoint]:
    """Convert open data email address to contact point and load to sink."""
    ldap = LDAPConnector.get()
    contact_point = [
        transform_ldap_functional_account_to_extracted_contact_point(
            ldap.get_functional_account(mail=OPEN_DATA_EMAIL),
            get_extracted_primary_source_id_by_name("ldap"),
        )
    ]

    load(contact_point)
    return contact_point


@asset(group_name="open_data")
def open_data_extracted_distributions(
    open_data_parent_resources: list[OpenDataParentResource],
) -> list[ExtractedDistribution]:
    """Extract distributions for open data & transform and load them to sinks."""
    settings = Settings.get()
    distribution_mapping = DistributionMapping.model_validate(
        load_yaml(settings.open_data.mapping_path / "distribution.yaml")
    )
    mex_distributions = transform_open_data_distributions(
        open_data_parent_resources,
        distribution_mapping,
    )

    load(mex_distributions)
    return mex_distributions


@asset(group_name="open_data")
def open_data_parent_extracted_resources(  # noqa: PLR0913
    open_data_parent_resources: list[OpenDataParentResource],
    open_data_extracted_persons: list[ExtractedPerson],
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
    unit_stable_target_ids_by_synonym: dict[
        str, list[MergedOrganizationalUnitIdentifier]
    ],
    open_data_extracted_distributions: list[ExtractedDistribution],
    extracted_organization_rki: ExtractedOrganization,
    open_data_extracted_contact_points: list[ExtractedContactPoint],
) -> list[ExtractedResource]:
    """Transform parent resources to extracted resources & load them to the sinks."""
    settings = Settings.get()
    resource_mapping = ResourceMapping.model_validate(
        load_yaml(settings.open_data.mapping_path / "resource.yaml")
    )

    mex_sources = transform_open_data_parent_resource_to_mex_resource(
        open_data_parent_resources,
        open_data_extracted_persons,
        unit_stable_target_ids_by_synonym,
        extracted_organizational_units,
        open_data_extracted_distributions,
        resource_mapping,
        extracted_organization_rki,
        open_data_extracted_contact_points,
    )

    load(mex_sources)
    return mex_sources


@asset(group_name="open_data")
def open_data_version_id_by_resource_id(
    open_data_parent_extracted_resources: list[ExtractedResource],
    open_data_parent_resources: list[OpenDataParentResource],
    open_data_extracted_distributions: list[ExtractedDistribution],
) -> dict[MergedResourceIdentifier, int]:
    """Get Zenodo version id (to download tableschema zip) per Resource stableTargetId.

    Info from open data team: for some resources there are no, for some there are
    other "wrong" metadata.zip files. If datapackage.json exists for a resource,
    there also is a metadata.zip and it is valid for our use case.
    """
    return {
        resource.stableTargetId: parent_resource.id
        for parent_resource in open_data_parent_resources
        for resource in open_data_parent_extracted_resources
        if resource.identifierInPrimarySource == parent_resource.conceptrecid
        for distribution in open_data_extracted_distributions
        if (
            distribution.stableTargetId in resource.distribution
            and distribution.title[0].value == "datapackage.json"
        )
    }


@asset(group_name="open_data")
def open_data_tableschemas_by_resource_id(
    open_data_version_id_by_resource_id: dict[MergedResourceIdentifier, int],
) -> dict[MergedResourceIdentifier, dict[str, list[OpenDataTableSchema]]]:
    """Extract and collect metadata zip tableschemas by resource stableTargetId."""
    return {
        resource_key: extract_tableschema(
            open_data_version_id_by_resource_id[resource_key]
        )
        for resource_key in open_data_version_id_by_resource_id
    }


@asset(group_name="open_data")
def open_data_extracted_variable_group(
    open_data_tableschemas_by_resource_id: dict[
        MergedResourceIdentifier, dict[str, list[OpenDataTableSchema]]
    ],
) -> list[ExtractedVariableGroup]:
    """Transform tableschema filenames to variable groups."""
    extracted_variable_groups = transform_open_data_variable_groups(
        open_data_tableschemas_by_resource_id,
    )
    load(extracted_variable_groups)
    return extracted_variable_groups


@asset(group_name="open_data")
def open_data_extracted_variables(
    open_data_tableschemas_by_resource_id: dict[
        MergedResourceIdentifier, dict[str, list[OpenDataTableSchema]]
    ],
    open_data_extracted_variable_group: list[ExtractedVariableGroup],
) -> list[ExtractedVariable]:
    """Transform tableschema file content to variables."""
    merged_variable_group_id_by_filename = {
        variable.identifierInPrimarySource: variable.stableTargetId
        for variable in open_data_extracted_variable_group
    }
    extracted_variables = transform_open_data_variables(
        open_data_tableschemas_by_resource_id, merged_variable_group_id_by_filename
    )
    load(extracted_variables)
    return extracted_variables


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the odk extractor job in-process."""
    run_job_in_process("open_data")
