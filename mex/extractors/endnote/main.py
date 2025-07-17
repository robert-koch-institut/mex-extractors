from dagster import asset

from mex.common.cli import entrypoint
from mex.common.models import (
    BibliographicResourceMapping,
    ConsentMapping,
    ExtractedPerson,
    ExtractedPrimarySource,
)
from mex.common.primary_source.transform import get_primary_sources_by_name
from mex.common.types import MergedOrganizationalUnitIdentifier
from mex.extractors.endnote.extract import extract_endnote_records
from mex.extractors.endnote.model import EndnoteRecord
from mex.extractors.endnote.transform import (
    extract_endnote_bibliographic_resource,
    extract_endnote_consents,
    extract_endnote_persons_by_person_string,
)
from mex.extractors.pipeline import run_job_in_process
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
def extracted_endnote_persons_by_person_string(
    endnote_records: list[EndnoteRecord],
    extracted_primary_source_endnote: ExtractedPrimarySource,
) -> dict[str, ExtractedPerson]:
    """Extract endnote persons by person string."""
    extracted_persons = extract_endnote_persons_by_person_string(
        endnote_records,
        extracted_primary_source_endnote,
    )
    load(extracted_persons.values())
    return extracted_persons


@asset(group_name="endnote")
def extracted_endnote_consents(
    extracted_endnote_persons_by_person_string: dict[str, ExtractedPerson],
    extracted_primary_source_endnote: ExtractedPrimarySource,
) -> None:
    """Extract records from endnote."""
    settings = Settings.get()
    endnote_consent_mapping = ConsentMapping.model_validate(
        load_yaml(settings.endnote.mapping_path / "consent.yaml")
    )
    extracted_endnote_consents = extract_endnote_consents(
        list(extracted_endnote_persons_by_person_string.values()),
        extracted_primary_source_endnote,
        endnote_consent_mapping,
    )
    load(extracted_endnote_consents)


@asset(group_name="endnote")
def extracted_endnote_bibliographic_resources(
    endnote_records: list[EndnoteRecord],
    extracted_endnote_persons_by_person_string: dict[str, ExtractedPerson],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_primary_source_endnote: ExtractedPrimarySource,
) -> None:
    """Extract bibliographic resources from endnote."""
    settings = Settings.get()
    endnote_bibliographic_resource_mapping = (
        BibliographicResourceMapping.model_validate(
            load_yaml(settings.endnote.mapping_path / "bibliographic-resource.yaml")
        )
    )
    extracted_bibliographic_resource = extract_endnote_bibliographic_resource(
        endnote_records,
        endnote_bibliographic_resource_mapping,
        extracted_endnote_persons_by_person_string,
        unit_stable_target_ids_by_synonym,
        extracted_primary_source_endnote,
    )
    load(extracted_bibliographic_resource)


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the endnote extractor job in-process."""
    run_job_in_process("endnote")
