from dagster import asset

from mex.common.models import ExtractedPrimarySource
from mex.common.primary_source.extract import extract_seed_primary_sources
from mex.common.primary_source.transform import (
    get_primary_sources_by_name,
    transform_seed_primary_sources_to_extracted_primary_sources,
)
from mex.extractors.sinks import load


@asset(group_name="default")
def extracted_primary_sources() -> list[ExtractedPrimarySource]:
    """Extract and transform all primary sources.

    Extract the primary sources from the raw-data JSON file and transform them into
    a list of ExtractedPrimarySources.

    Returns:
        List of all ExtractedPrimarySources
    """
    seed_primary_sources = extract_seed_primary_sources()
    return transform_seed_primary_sources_to_extracted_primary_sources(
        seed_primary_sources
    )


@asset(group_name="default")
def extracted_primary_source_mex(
    extracted_primary_sources: list[ExtractedPrimarySource],
) -> ExtractedPrimarySource:
    """Load and return MEx primary source."""
    (extracted_primary_source_mex,) = get_primary_sources_by_name(
        extracted_primary_sources,
        "mex",
    )
    load([extracted_primary_source_mex])
    return extracted_primary_source_mex


@asset(group_name="default", deps=["extracted_primary_source_mex"])
def extracted_primary_source_ldap(
    extracted_primary_sources: list[ExtractedPrimarySource],
) -> ExtractedPrimarySource:
    """Load and return ldap primary source."""
    (extracted_primary_source_ldap,) = get_primary_sources_by_name(
        extracted_primary_sources,
        "ldap",
    )
    load([extracted_primary_source_ldap])
    return extracted_primary_source_ldap


@asset(group_name="default", deps=["extracted_primary_source_mex"])
def extracted_primary_source_organigram(
    extracted_primary_sources: list[ExtractedPrimarySource],
) -> ExtractedPrimarySource:
    """Load and return organigram primary source."""
    (extracted_primary_source_organigram,) = get_primary_sources_by_name(
        extracted_primary_sources,
        "organigram",
    )
    load([extracted_primary_source_organigram])
    return extracted_primary_source_organigram


@asset(group_name="default", deps=["extracted_primary_source_mex"])
def extracted_primary_source_report_server(
    extracted_primary_sources: list[ExtractedPrimarySource],
) -> ExtractedPrimarySource:
    """Load and return report-server primary source."""
    (extracted_primary_source_report_server,) = get_primary_sources_by_name(
        extracted_primary_sources,
        "report-server",
    )
    load([extracted_primary_source_report_server])
    return extracted_primary_source_report_server


@asset(group_name="default", deps=["extracted_primary_source_mex"])
def extracted_primary_source_wikidata(
    extracted_primary_sources: list[ExtractedPrimarySource],
) -> ExtractedPrimarySource:
    """Load and return wikidata primary source."""
    (extracted_primary_source_wikidata,) = get_primary_sources_by_name(
        extracted_primary_sources,
        "wikidata",
    )
    load([extracted_primary_source_wikidata])
    return extracted_primary_source_wikidata
