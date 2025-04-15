from mex.common.cli import entrypoint
from mex.common.models import (
    ConsentMapping,
    ExtractedOrganizationalUnit,
    ExtractedPerson,
    ExtractedPrimarySource,
)
from mex.common.primary_source.transform import get_primary_sources_by_name
from mex.extractors.endnote.extract import (
    extract_endnote_records,
)
from mex.extractors.endnote.model import EndnoteRecord
from mex.extractors.endnote.transform import (
    extract_endnote_consents,
    extract_endnote_persons,
)
from mex.extractors.pipeline import asset, run_job_in_process
from mex.extractors.settings import Settings
from mex.extractors.sinks import load
from mex.extractors.utils import load_yaml


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


@asset(group_name="endnote")
def extracted_endnote_persons(
    endnote_records: list[EndnoteRecord],
    extracted_primary_source_ldap: ExtractedPrimarySource,
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
) -> list[ExtractedPerson]:
    """Extract records from endnote."""
    extracted_persons = extract_endnote_persons(
        endnote_records,
        extracted_primary_source_ldap,
        extracted_organizational_units,
    )
    load(extracted_persons)
    return extracted_persons


@asset(group_name="endnote")
def extracted_endnote_consents(
    extracted_endnote_persons: list[ExtractedPerson],
    extracted_primary_source_endnote: ExtractedPrimarySource,
) -> None:
    """Extract records from endnote."""
    settings = Settings.get()
    endnote_consent_mapping = ConsentMapping.model_validate(
        load_yaml(settings.open_data.mapping_path / "consent.yaml")
    )
    extracted_endnote_consents = extract_endnote_consents(
        extracted_endnote_persons,
        extracted_primary_source_endnote,
        endnote_consent_mapping,
    )
    load(extracted_endnote_consents)


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the endnote extractor job in-process."""
    run_job_in_process("endnote")
