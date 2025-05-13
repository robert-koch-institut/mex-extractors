from dagster import asset

from mex.common.cli import entrypoint
from mex.common.models import (
    ExtractedPrimarySource,
)
from mex.common.primary_source.transform import get_primary_sources_by_name
from mex.extractors.endnote.extract import (
    extract_endnote_records,
)
from mex.extractors.endnote.model import EndnoteRecord
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.settings import Settings
from mex.extractors.sinks import load


@asset(group_name="endnote", deps=["extracted_primary_source_mex"])
def extracted_primary_source_endnote(
    extracted_primary_sources: list[ExtractedPrimarySource],
) -> ExtractedPrimarySource:
    """Load and return endnote primary source."""
    (extracted_primary_source,) = get_primary_sources_by_name(
        extracted_primary_sources, "endnote"
    )
    load([extracted_primary_source])

    return extracted_primary_source


@asset(group_name="endnote")
def endnote_records() -> list[EndnoteRecord]:
    """Extract records from endnote."""
    return extract_endnote_records()


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the endnote extractor job in-process."""
    run_job_in_process("endnote")
