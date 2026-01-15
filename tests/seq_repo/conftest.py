import pytest
from pytest import MonkeyPatch

from mex.common.ldap.models import LDAPPerson, LDAPPersonWithQuery
from mex.common.models import (
    AccessPlatformMapping,
    ActivityMapping,
    ExtractedAccessPlatform,
    ExtractedActivity,
    ResourceMapping,
)
from mex.common.types import MergedOrganizationalUnitIdentifier, MergedPersonIdentifier
from mex.extractors.seq_repo import extract
from mex.extractors.seq_repo.filter import filter_sources_on_latest_sequencing_date
from mex.extractors.seq_repo.model import SeqRepoSource
from mex.extractors.seq_repo.transform import (
    transform_seq_repo_access_platform_to_extracted_access_platform,
    transform_seq_repo_activities_to_extracted_activities,
)
from mex.extractors.settings import Settings
from mex.extractors.utils import load_yaml


@pytest.fixture
def seq_repo_sources() -> list[SeqRepoSource]:
    return [
        SeqRepoSource(
            project_coordinators=["test_person", "test_person"],
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
            project_coordinators=["juturna", "felicitas"],
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
) -> dict[str, SeqRepoSource]:
    return filter_sources_on_latest_sequencing_date(seq_repo_sources)


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
    seq_repo_access_platform: AccessPlatformMapping,
) -> ExtractedAccessPlatform:
    return transform_seq_repo_access_platform_to_extracted_access_platform(
        seq_repo_access_platform,
    )


@pytest.fixture
def extracted_mex_activities_dict(
    seq_repo_latest_sources: dict[str, SeqRepoSource],
    seq_repo_activity: ActivityMapping,
    seq_repo_ldap_persons_with_query: list[LDAPPersonWithQuery],
    seq_repo_merged_person_ids_by_query_string: dict[str, list[MergedPersonIdentifier]],
) -> dict[str, ExtractedActivity]:
    extracted_mex_activities = transform_seq_repo_activities_to_extracted_activities(
        seq_repo_latest_sources,
        seq_repo_activity,
        seq_repo_ldap_persons_with_query,
        seq_repo_merged_person_ids_by_query_string,
    )
    return {
        activity.identifierInPrimarySource: activity
        for activity in extracted_mex_activities
    }


@pytest.fixture
def seq_repo_ldap_persons_with_query(
    ldap_roland_resolved: LDAPPerson,
) -> list[LDAPPersonWithQuery]:
    """Extract source project coordinators."""
    return [LDAPPersonWithQuery(person=ldap_roland_resolved, query="resolvedr")]


@pytest.fixture
def seq_repo_merged_person_ids_by_query_string() -> dict[
    str, list[MergedPersonIdentifier]
]:
    """Get project coordinators merged ids."""
    return {
        "resolvedr": [MergedPersonIdentifier("d6Lni0XPiEQM5jILEBOYxO")],
        "juturna": [MergedPersonIdentifier("buTvstFluFUX9TeoHlhe7c")],
        "felicitas": [MergedPersonIdentifier("gOwHDDA0HQgT1eDYnC4Ai5")],
    }


@pytest.fixture
def unit_stable_target_ids_by_synonym() -> dict[
    str, list[MergedOrganizationalUnitIdentifier]
]:
    """Extract the dummy units and return them grouped by synonyms."""
    return {
        "child-unit": [MergedOrganizationalUnitIdentifier("g2AinFG4E6n8H1ZMuaBW6o")],
        "CHLD Unterabteilung": [
            MergedOrganizationalUnitIdentifier("g2AinFG4E6n8H1ZMuaBW6o")
        ],
        "C1: Sub Unit": [MergedOrganizationalUnitIdentifier("g2AinFG4E6n8H1ZMuaBW6o")],
        "C1": [MergedOrganizationalUnitIdentifier("g2AinFG4E6n8H1ZMuaBW6o")],
        "CHLD": [MergedOrganizationalUnitIdentifier("g2AinFG4E6n8H1ZMuaBW6o")],
        "C1 Sub-Unit": [MergedOrganizationalUnitIdentifier("g2AinFG4E6n8H1ZMuaBW6o")],
        "C1 Unterabteilung": [
            MergedOrganizationalUnitIdentifier("g2AinFG4E6n8H1ZMuaBW6o")
        ],
        "parent-unit": [MergedOrganizationalUnitIdentifier("dLqCAZCHhjZmJcJR98ytzQ")],
        "Abteilung": [MergedOrganizationalUnitIdentifier("dLqCAZCHhjZmJcJR98ytzQ")],
        "Department": [MergedOrganizationalUnitIdentifier("dLqCAZCHhjZmJcJR98ytzQ")],
        "PRNT": [MergedOrganizationalUnitIdentifier("dLqCAZCHhjZmJcJR98ytzQ")],
        "PRNT Abteilung": [
            MergedOrganizationalUnitIdentifier("dLqCAZCHhjZmJcJR98ytzQ")
        ],
        "PARENT Dept.": [MergedOrganizationalUnitIdentifier("dLqCAZCHhjZmJcJR98ytzQ")],
        "fg99": [MergedOrganizationalUnitIdentifier("e4fyMCGjCeQNSvAMNHcBhK")],
        "Fachgebiet 99": [MergedOrganizationalUnitIdentifier("e4fyMCGjCeQNSvAMNHcBhK")],
        "Group 99": [MergedOrganizationalUnitIdentifier("e4fyMCGjCeQNSvAMNHcBhK")],
        "FG 99": [MergedOrganizationalUnitIdentifier("e4fyMCGjCeQNSvAMNHcBhK")],
        "FG99": [MergedOrganizationalUnitIdentifier("e4fyMCGjCeQNSvAMNHcBhK")],
    }


@pytest.fixture
def mock_email_domain(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setattr(extract, "SEQ_REPO_EMAIL_DOMAIN", "email.de")
