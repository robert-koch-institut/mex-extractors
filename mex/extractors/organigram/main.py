from mex.common.cli import entrypoint
from mex.common.organigram.extract import extract_organigram_units
from mex.common.organigram.transform import (
    transform_organigram_units_to_organizational_units,
)
from mex.common.primary_source.extract import extract_seed_primary_sources
from mex.common.primary_source.transform import (
    get_primary_sources_by_name,
    transform_seed_primary_sources_to_extracted_primary_sources,
)
from mex.extractors.pipeline import asset, run_job_in_process
from mex.extractors.settings import Settings
from mex.extractors.sinks import load


@asset(group_name="organigram")
def extract_organigram() -> None:
    """Run the organigram extractor.

    This involves parsing the organigram file, transformation to Metadata Exchange (MEx)
    format and writing the results to files or the configured API.
    """
    seed_primary_sources = extract_seed_primary_sources()
    extracted_primary_sources = (
        transform_seed_primary_sources_to_extracted_primary_sources(
            seed_primary_sources
        )
    )
    (
        extracted_primary_source_mex,
        extracted_primary_source_organigram,
    ) = get_primary_sources_by_name(extracted_primary_sources, "mex", "organigram")
    load(
        [
            extracted_primary_source_mex,
            extracted_primary_source_organigram,
        ]
    )

    organigram_units = extract_organigram_units()
    extracted_units = transform_organigram_units_to_organizational_units(
        organigram_units, extracted_primary_source_organigram
    )
    load(extracted_units)


@entrypoint(Settings)
def run() -> None:
    """Run the organigram extractor job in-process."""
    run_job_in_process("organigram")
