from typing import TYPE_CHECKING

import pytest

from mex.common.ldap.models import LDAPPerson, LDAPPersonWithQuery
from mex.common.models import (
    AccessPlatformMapping,
    ActivityMapping,
    ExtractedAccessPlatform,
    ExtractedActivity,
    ExtractedPerson,
    ResourceMapping,
)
from mex.extractors.seq_repo.filter import filter_sources_on_latest_sequencing_date
from mex.extractors.seq_repo.model import SeqRepoSource
from mex.extractors.seq_repo.transform import (
    transform_seq_repo_access_platform_to_extracted_access_platform,
    transform_seq_repo_activities_to_extracted_activities,
)
from mex.extractors.utils import load_yaml

if TYPE_CHECKING:
    from mex.common.types import MergedPersonIdentifier
    from mex.extractors.settings import Settings


@pytest.fixture
def seq_repo_sources() -> list[SeqRepoSource]:
    return [
        SeqRepoSource(
            project_coordinators=["FictitiousF", "ResolvedR"],
            customer_org_unit_id="FG99",
            sequencing_date="2023-08-07",
            lims_sample_id="test-sample-id",
            sequencing_platform="TEST",
            species="virus XYZ",
            project_name="FG99-ABC-123",
            customer_sample_name="test-customer-name-1",
            project_id="TEST-ID",
        ),
        SeqRepoSource(
            project_coordinators=["FelicitasJ", "NonExistent"],
            customer_org_unit_id="FG99",
            sequencing_date="2023-08-07",
            lims_sample_id="test-sample-id",
            sequencing_platform="TEST",
            species="Lab rat",
            project_name="FG99-ABC-321",
            customer_sample_name="test-customer-name-2",
            project_id="TEST-ID",
        ),
        SeqRepoSource(
            project_coordinators=["ResolvedR"],
            customer_org_unit_id=None,
            sequencing_date="1970-01-01",
            lims_sample_id="another-test-sample-id",
            sequencing_platform="TEST-2",
            species="guniea pig",
            project_name="FG99-ABC-789",
            customer_sample_name="test-customer-name-3",
            project_id="TEST-ID-2",
        ),
        SeqRepoSource(
            customer_org_unit_id="FG99",
            customer_sample_name="test-customer-name-2",
            lims_sample_id="test-sample-id",
            project_coordinators=["ResolvedR"],
            project_id="TEST-ID",
            project_name="SKIPPED BECAUSE MISSING DATE",
            sequenced_sample_id="SEQ-321",
            sequencing_platform="TEST",
            species="Lab rat",
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
    return [LDAPPersonWithQuery(person=ldap_roland_resolved, query="ResolvedR")]


@pytest.fixture
def seq_repo_merged_person_ids_by_query_string(
    roland_resolved: ExtractedPerson,
    juturna_felicitas: ExtractedPerson,
    frieda_fictitious: ExtractedPerson,
) -> dict[str, list[MergedPersonIdentifier]]:
    """Get project coordinators merged ids."""
    return {
        "ResolvedR": [roland_resolved.stableTargetId],
        "FelicitasJ": [juturna_felicitas.stableTargetId],
        "FictitiousF": [frieda_fictitious.stableTargetId],
    }
