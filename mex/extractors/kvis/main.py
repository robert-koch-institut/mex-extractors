from dagster import asset

from mex.common.cli import entrypoint
from mex.extractors.kvis.extract import extract_sql_table
from mex.extractors.kvis.models.table_models import (
    KVISFieldValuesTable,
    KVISVariablesTable,
)
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.settings import Settings


@asset(group_name="kvis")
def kvis_variables_table() -> list[KVISVariablesTable]:
    """Extract SQL Server KVIS Variables Table."""
    return extract_sql_table(KVISVariablesTable)


@asset(group_name="kvis")
def kvis_field_values_table() -> list[KVISFieldValuesTable]:
    """Extract SQL Server KVIS FieldValues Table."""
    return extract_sql_table(KVISFieldValuesTable)


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the KVIS extractor job in-process."""
    run_job_in_process("kvis")
