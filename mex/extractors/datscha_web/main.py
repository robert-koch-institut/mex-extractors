from dagster import asset

from mex.common.cli import entrypoint
from mex.common.ldap.extract import get_merged_ids_by_query_string
from mex.common.ldap.transform import (
    transform_ldap_persons_with_query_to_extracted_persons,
)
from mex.common.models import (
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedOrganizationalUnit,
)
from mex.common.types import (
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
)
from mex.extractors.datscha_web.extract import (
    extract_datscha_web_items,
    extract_datscha_web_organizations,
    extract_datscha_web_source_contacts,
)
from mex.extractors.datscha_web.models.item import DatschaWebItem
from mex.extractors.datscha_web.transform import (
    transform_datscha_web_items_to_mex_activities,
)
from mex.extractors.filters import filter_by_global_rules
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.settings import Settings
from mex.extractors.sinks import load


@asset(group_name="datscha_web")
def datscha_web_items() -> list[DatschaWebItem]:
    """Return extracted items from Datscha Web."""
    datscha_web_items = extract_datscha_web_items()
    return filter_by_global_rules(
        get_extracted_primary_source_id_by_name("datscha-web"),
        datscha_web_items,
    )


@asset(group_name="datscha_web")
def datscha_web_person_ids_by_query_str(
    datscha_web_items: list[DatschaWebItem],
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
    extracted_organization_rki: ExtractedOrganization,
) -> dict[str, list[MergedPersonIdentifier]]:
    """Extract Datscha Web contact persons from LDAP and return them by query string."""
    ldap_source_contacts = extract_datscha_web_source_contacts(datscha_web_items)
    ldap_primary_source_id = get_extracted_primary_source_id_by_name("ldap")
    mex_source_contacts = transform_ldap_persons_with_query_to_extracted_persons(
        ldap_source_contacts,
        ldap_primary_source_id,
        extracted_organizational_units,
        extracted_organization_rki,
    )
    load(mex_source_contacts)

    return get_merged_ids_by_query_string(ldap_source_contacts, ldap_primary_source_id)


@asset(group_name="datscha_web")
def datscha_web_organization_ids_by_query_str(
    datscha_web_items: list[DatschaWebItem],
) -> dict[str, MergedOrganizationIdentifier]:
    """Extract organizations for Datscha Web from wikidata and group them by query."""
    return extract_datscha_web_organizations(datscha_web_items)


@asset(group_name="datscha_web")
def datscha_web_extracted_activities(
    datscha_web_items: list[DatschaWebItem],
    datscha_web_person_ids_by_query_str: dict[str, list[MergedPersonIdentifier]],
    datscha_web_organization_ids_by_query_str: dict[str, MergedOrganizationIdentifier],
) -> list[ExtractedActivity]:
    """Transform Datscha Web to extracted sources and load them to the sinks."""
    mex_sources = transform_datscha_web_items_to_mex_activities(
        datscha_web_items,
        datscha_web_person_ids_by_query_str,
        datscha_web_organization_ids_by_query_str,
    )
    load(mex_sources)
    return mex_sources


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the datscha-web extractor job in-process."""
    run_job_in_process("datscha_web")
