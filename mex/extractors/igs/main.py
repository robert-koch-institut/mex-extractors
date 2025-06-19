from dagster import asset

from mex.common.cli import entrypoint
from mex.common.models import (
    ExtractedPrimarySource,
)
from mex.common.primary_source.transform import get_primary_sources_by_name
from mex.extractors.igs.extract import extract_igs_schemas
from mex.extractors.igs.model import IGSSchemas
from mex.extractors.pipeline.base import run_job_in_process
from mex.extractors.settings import Settings
from mex.extractors.sinks import load


@asset(group_name="igs", deps=["extracted_primary_source_mex"])
def extracted_primary_source_igs(
    extracted_primary_sources: list[ExtractedPrimarySource],
) -> ExtractedPrimarySource:
    """Load and return IGS primary source."""
    (extracted_primary_source,) = get_primary_sources_by_name(
        extracted_primary_sources, "igs"
    )
    load([extracted_primary_source])
    return extracted_primary_source


@asset(group_name="igs")
def igs_schemas() -> dict[str, IGSSchemas]:
    """Extract from IGS schemas."""
    return extract_igs_schemas()


@entrypoint(Settings)
def run() -> None:
    """Run the IGS extractor job in-process."""
    run_job_in_process("igs")
