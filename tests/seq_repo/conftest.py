from uuid import UUID

import pytest

from mex.common.ldap.models import LDAPPerson, LDAPPersonWithQuery
from mex.common.models import (
    AccessPlatformMapping,
    ActivityMapping,
    ExtractedAccessPlatform,
    ExtractedActivity,
    ExtractedPrimarySource,
    ResourceMapping,
)
from mex.common.primary_source.extract import extract_seed_primary_sources
from mex.common.primary_source.transform import (
    get_primary_sources_by_name,
    transform_seed_primary_sources_to_extracted_primary_sources,
)
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedPersonIdentifier,
)
from mex.extractors.seq_repo.filter import filter_sources_on_latest_sequencing_date
from mex.extractors.seq_repo.model import SeqRepoSource
from mex.extractors.seq_repo.transform import (
    transform_seq_repo_access_platform_to_extracted_access_platform,
    transform_seq_repo_activities_to_extracted_activities,
)
from mex.extractors.settings import Settings
from mex.extractors.utils import load_yaml


@pytest.fixture(autouse=True)
def seq_repo_extracted_primary_source() -> ExtractedPrimarySource:
    seed_primary_sources = extract_seed_primary_sources()
    extracted_primary_sources = (
        transform_seed_primary_sources_to_extracted_primary_sources(
            seed_primary_sources
        )
    )
    (seq_repo_extracted_primary_source,) = get_primary_sources_by_name(
        extracted_primary_sources,
        "seq-repo",
    )
    return seq_repo_extracted_primary_source


@pytest.fixture
def seq_repo_sources() -> list[SeqRepoSource]:
    return [
        SeqRepoSource(
            project_coordinators=["max", "mustermann", "max"],
            customer_org_unit_id="FG99",
            sequencing_date="2023-08-07",
            lims_sample_id="test-sample-id",
            sequencing_platform="TEST",
            species="Severe acute respiratory syndrome coronavirus 2",
            project_name="FG99-ABC-123",
            customer_sample_name="test-customer-name-1",
            project_id="TEST-ID",
        ),
        SeqRepoSource(
            project_coordinators=["jelly", "fish", "turtle"],
            customer_org_unit_id="FG99",
            sequencing_date="2023-08-07",
            lims_sample_id="test-sample-id",
            sequencing_platform="TEST",
            species="Lab rat",
            project_name="FG99-ABC-321",
            customer_sample_name="test-customer-name-2",
            project_id="TEST-ID",
        ),
    ]


@pytest.fixture
def seq_repo_latest_sources(
    seq_repo_sources: list[SeqRepoSource],
    seq_repo_extracted_primary_source: ExtractedPrimarySource,
) -> dict[str, SeqRepoSource]:
    return filter_sources_on_latest_sequencing_date(
        seq_repo_sources, seq_repo_extracted_primary_source
    )


@pytest.fixture
def seq_repo_activity(settings: Settings) -> ActivityMapping:
    return ActivityMapping.model_validate(
        load_yaml(settings.seq_repo.mapping_path / "activity_mock.yaml")
    )


@pytest.fixture
def seq_repo_access_platform(settings: Settings) -> AccessPlatformMapping:
    return AccessPlatformMapping.model_validate(
        load_yaml(settings.seq_repo.mapping_path / "access-platform_mock.yaml")
    )


@pytest.fixture
def seq_repo_resource(settings: Settings) -> ResourceMapping:
    return ResourceMapping.model_validate(
        load_yaml(settings.seq_repo.mapping_path / "resource_mock.yaml")
    )


@pytest.fixture
def extracted_mex_access_platform(
    seq_repo_extracted_primary_source: ExtractedPrimarySource,
    seq_repo_access_platform: AccessPlatformMapping,
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> ExtractedAccessPlatform:
    return transform_seq_repo_access_platform_to_extracted_access_platform(
        seq_repo_access_platform,
        unit_stable_target_ids_by_synonym,
        seq_repo_extracted_primary_source,
    )


@pytest.fixture
def extracted_mex_activities_dict(  # noqa: PLR0913
    seq_repo_extracted_primary_source: ExtractedPrimarySource,
    seq_repo_latest_sources: dict[str, SeqRepoSource],
    seq_repo_activity: ActivityMapping,
    seq_repo_ldap_persons_with_query: list[LDAPPersonWithQuery],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    seq_repo_merged_person_ids_by_query_string: dict[str, list[MergedPersonIdentifier]],
) -> dict[str, ExtractedActivity]:
    extracted_mex_activities = transform_seq_repo_activities_to_extracted_activities(
        seq_repo_latest_sources,
        seq_repo_activity,
        seq_repo_ldap_persons_with_query,
        unit_stable_target_ids_by_synonym,
        seq_repo_merged_person_ids_by_query_string,
        seq_repo_extracted_primary_source,
    )
    return {
        activity.identifierInPrimarySource: activity
        for activity in extracted_mex_activities
    }


@pytest.fixture
def seq_repo_ldap_persons_with_query() -> list[LDAPPersonWithQuery]:
    """Extract source project coordinators."""
    return [
        LDAPPersonWithQuery(
            person=LDAPPerson(
                sAMAccountName="max",
                objectGUID=UUID("00000000-0000-4000-8000-000000000004"),
                mail=[],
                company=None,
                department="FG99",
                departmentNumber="FG99",
                displayName="mustermann, max",
                employeeID="42",
                givenName=["max"],
                ou=[],
                sn="mustermann",
            ),
            query="max",
        ),
        LDAPPersonWithQuery(
            person=LDAPPerson(
                sAMAccountName="max",
                objectGUID=UUID("00000000-0000-4000-8000-000000000004"),
                mail=[],
                company=None,
                department="FG99",
                departmentNumber="FG99",
                displayName="mustermann, max",
                employeeID="42",
                givenName=["max"],
                ou=[],
                sn="mustermann",
            ),
            query="mustermann",
        ),
    ]


@pytest.fixture
def seq_repo_merged_person_ids_by_query_string() -> dict[
    str, list[MergedPersonIdentifier]
]:
    """Get project coordinators merged ids."""
    return {
        "mustermann": [MergedPersonIdentifier("e0Rxxm9WvnMqPLZ44UduNx")],
        "max": [MergedPersonIdentifier("d6Lni0XPiEQM5jILEBOYxO")],
        "jelly": [MergedPersonIdentifier("buTvstFluFUX9TeoHlhe7c")],
        "fish": [MergedPersonIdentifier("gOwHDDA0HQgT1eDYnC4Ai5")],
    }


@pytest.fixture
def unit_stable_target_ids_by_synonym() -> dict[
    str, MergedOrganizationalUnitIdentifier
]:
    """Extract the dummy units and return them grouped by synonyms."""
    return {
        "child-unit": MergedOrganizationalUnitIdentifier("g2AinFG4E6n8H1ZMuaBW6o"),
        "CHLD Unterabteilung": MergedOrganizationalUnitIdentifier(
            "g2AinFG4E6n8H1ZMuaBW6o"
        ),
        "C1: Sub Unit": MergedOrganizationalUnitIdentifier("g2AinFG4E6n8H1ZMuaBW6o"),
        "C1": MergedOrganizationalUnitIdentifier("g2AinFG4E6n8H1ZMuaBW6o"),
        "CHLD": MergedOrganizationalUnitIdentifier("g2AinFG4E6n8H1ZMuaBW6o"),
        "C1 Sub-Unit": MergedOrganizationalUnitIdentifier("g2AinFG4E6n8H1ZMuaBW6o"),
        "C1 Unterabteilung": MergedOrganizationalUnitIdentifier(
            "g2AinFG4E6n8H1ZMuaBW6o"
        ),
        "parent-unit": MergedOrganizationalUnitIdentifier("dLqCAZCHhjZmJcJR98ytzQ"),
        "Abteilung": MergedOrganizationalUnitIdentifier("dLqCAZCHhjZmJcJR98ytzQ"),
        "Department": MergedOrganizationalUnitIdentifier("dLqCAZCHhjZmJcJR98ytzQ"),
        "PRNT": MergedOrganizationalUnitIdentifier("dLqCAZCHhjZmJcJR98ytzQ"),
        "PRNT Abteilung": MergedOrganizationalUnitIdentifier("dLqCAZCHhjZmJcJR98ytzQ"),
        "PARENT Dept.": MergedOrganizationalUnitIdentifier("dLqCAZCHhjZmJcJR98ytzQ"),
        "fg99": MergedOrganizationalUnitIdentifier("e4fyMCGjCeQNSvAMNHcBhK"),
        "Fachgebiet 99": MergedOrganizationalUnitIdentifier("e4fyMCGjCeQNSvAMNHcBhK"),
        "Group 99": MergedOrganizationalUnitIdentifier("e4fyMCGjCeQNSvAMNHcBhK"),
        "FG 99": MergedOrganizationalUnitIdentifier("e4fyMCGjCeQNSvAMNHcBhK"),
        "FG99": MergedOrganizationalUnitIdentifier("e4fyMCGjCeQNSvAMNHcBhK"),
    }
