from typing import TYPE_CHECKING

import pytest

from mex.common.ldap.models import LDAPPerson, LDAPPersonWithQuery
from mex.common.models import (
    AccessPlatformMapping,
    ActivityMapping,
    ExtractedAccessPlatform,
    ExtractedActivity,
    ResourceMapping,
)
from mex.extractors.seq_repo.model import SeqRepoSource
from mex.extractors.seq_repo.transform import (
    transform_seq_repo_access_platform_to_extracted_access_platform,
    transform_seq_repo_activities_to_extracted_activities,
)
from mex.extractors.utils import load_yaml

if TYPE_CHECKING:
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
            basepair_count=1,
            reads_count=2,
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
            basepair_count=3,
            reads_count=4,
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
            basepair_count=5,
            reads_count=6,
        ),
        SeqRepoSource(
            customer_org_unit_id="FG99",
            customer_sample_name="test-customer-name-2",
            lims_sample_id="test-sample-id",
            project_coordinators=["ResolvedR"],
            project_id="TEST-ID",
            project_name="SKIPPED BECAUSE MISSING DATE",
            sequencing_platform="TEST",
            species="Lab rat",
            basepair_count=7,
            reads_count=8,
        ),
    ]


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
    seq_repo_sources: list[SeqRepoSource],
    seq_repo_activity: ActivityMapping,
) -> dict[str, ExtractedActivity]:
    extracted_mex_activities = transform_seq_repo_activities_to_extracted_activities(
        seq_repo_sources,
        seq_repo_activity,
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
