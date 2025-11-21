from dagster import MetadataValue, Output, asset

from mex.common.cli import entrypoint
from mex.common.models import (
    BibliographicResourceMapping,
    ConsentMapping,
    ExtractedConsent,
    ExtractedPerson,
)
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


@asset(group_name="endnote")
def endnote_records() -> list[EndnoteRecord]:
    """Extract records from endnote."""
    return extract_endnote_records()


@asset(group_name="endnote")
def endnote_extracted_persons_by_name_str(
    endnote_records: list[EndnoteRecord],
) -> dict[str, ExtractedPerson]:
    """Extract endnote persons by person string."""
    extracted_persons = extract_endnote_persons_by_person_string(
        endnote_records,
    )
    load(extracted_persons.values())
    return extracted_persons


@asset(group_name="endnote")
def endnote_extracted_consents(
    endnote_extracted_persons_by_name_str: dict[str, ExtractedPerson],
) -> list[ExtractedConsent]:
    """Extract records from endnote."""
    settings = Settings.get()
    endnote_consent_mapping = ConsentMapping.model_validate(
        load_yaml(settings.endnote.mapping_path / "consent.yaml")
    )
    endnote_extracted_consents = extract_endnote_consents(
        list(endnote_extracted_persons_by_name_str.values()),
        endnote_consent_mapping,
    )
    load(endnote_extracted_consents)
    return endnote_extracted_consents


@asset(group_name="endnote")
def endnote_extracted_bibliographic_resources(
    endnote_records: list[EndnoteRecord],
    endnote_extracted_persons_by_name_str: dict[str, ExtractedPerson],
    unit_stable_target_ids_by_synonym: dict[
        str, list[MergedOrganizationalUnitIdentifier]
        ],
) -> Output[int]:
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
        endnote_extracted_persons_by_name_str,
        unit_stable_target_ids_by_synonym,
    )
    num_items = len(extracted_bibliographic_resource)
    load(extracted_bibliographic_resource)
    return Output(value=num_items, metadata={"num_items": MetadataValue.int(num_items)})


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the endnote extractor job in-process."""
    run_job_in_process("endnote")
