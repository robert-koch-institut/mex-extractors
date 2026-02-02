from dagster import asset

from mex.common.cli import entrypoint
from mex.common.models import ExtractedResource
from mex.extractors.kvis.extract import extract_sql_table
from mex.extractors.kvis.models.table_models import (
    KVISFieldValues,
    KVISVariables,
)
from mex.extractors.kvis.transform import transform_kvis_resource_to_extracted_resource
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.settings import Settings


@asset(group_name="kvis")
def kvis_variables_table_entries() -> list[KVISVariables]:
    """Extract SQL Server KVIS Variables Table entries."""
    return extract_sql_table(KVISVariables)


@asset(group_name="kvis")
def kvis_fieldvalues_table_entries() -> list[KVISFieldValues]:
    """Extract SQL Server KVIS FieldValues Table entries."""
    return extract_sql_table(KVISFieldValues)


@asset(group_name="kvis")
def kvis_extracted_resource() -> ExtractedResource:
    """Create and return an extracted resource."""
    return transform_kvis_resource_to_extracted_resource()


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the KVIS extractor job in-process."""
    run_job_in_process("kvis")
