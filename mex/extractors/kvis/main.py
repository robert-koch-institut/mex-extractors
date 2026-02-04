from dagster import asset

from mex.common.cli import entrypoint
from mex.common.models import (
    ExtractedOrganization,
    ExtractedOrganizationalUnit,
    ExtractedVariable,
    ExtractedVariableGroup,
)
from mex.common.types import MergedResourceIdentifier
from mex.extractors.kvis.extract import extract_sql_table
from mex.extractors.kvis.models.table_models import (
    KVISFieldValues,
    KVISVariables,
)
from mex.extractors.kvis.transform import (
    transform_kvis_resource_to_extracted_resource,
    transform_kvis_table_entries_to_extracted_variables,
    transform_kvis_variables_to_extracted_variable_groups,
)
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.settings import Settings
from mex.extractors.sinks import load


@asset(group_name="kvis")
def kvis_variables_table_entries() -> list[KVISVariables]:
    """Extract SQL Server KVIS Variables Table entries."""
    return extract_sql_table(KVISVariables)


@asset(group_name="kvis")
def kvis_fieldvalues_table_entries() -> list[KVISFieldValues]:
    """Extract SQL Server KVIS FieldValues Table entries."""
    return extract_sql_table(KVISFieldValues)


@asset(group_name="kvis")
def kvis_extracted_resource_id(
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
    extracted_organization_rki: ExtractedOrganization,
) -> MergedResourceIdentifier:
    """Create extracted resource and return its stableTargetID."""
    extracted_resource = transform_kvis_resource_to_extracted_resource(
        extracted_organizational_units, extracted_organization_rki
    )
    load([extracted_resource])
    return extracted_resource.stableTargetId


@asset(group_name="kvis")
def kvis_extracted_variable_groups(
    kvis_extracted_resource_id: MergedResourceIdentifier,
    kvis_variables_table_entries: list[KVISVariables],
) -> list[ExtractedVariableGroup]:
    """Transform and load extracted variable groups."""
    extracted_variable_groups = transform_kvis_variables_to_extracted_variable_groups(
        kvis_extracted_resource_id, kvis_variables_table_entries
    )
    load(extracted_variable_groups)
    return extracted_variable_groups


@asset(group_name="kvis")
def kvis_extracted_variables(
    kvis_extracted_resource_id: MergedResourceIdentifier,
    kvis_extracted_variable_groups: list[ExtractedVariableGroup],
    kvis_variables_table_entries: list[KVISVariables],
    kvis_fieldvalues_table_entries: list[KVISFieldValues],
) -> list[ExtractedVariable]:
    """Transform and load extracted variables."""
    extracted_variables = transform_kvis_table_entries_to_extracted_variables(
        kvis_extracted_resource_id,
        kvis_extracted_variable_groups,
        kvis_variables_table_entries,
        kvis_fieldvalues_table_entries,
    )
    load(extracted_variables)
    return extracted_variables


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the KVIS extractor job in-process."""
    run_job_in_process("kvis")
