from typing import Any

import pytest

from mex.common.ldap.models.person import LDAPPersonWithQuery
from mex.common.models import (
    ExtractedAccessPlatform,
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedPrimarySource,
)
from mex.common.testing import Joker
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedPersonIdentifier,
    TextLanguage,
    YearMonthDay,
)
from mex.extractors.seq_repo.model import SeqRepoSource
from mex.extractors.seq_repo.transform import (
    transform_seq_repo_access_platform_to_extracted_access_platform,
    transform_seq_repo_activities_to_extracted_activities,
    transform_seq_repo_resource_to_extracted_resource,
)


@pytest.mark.usefixtures(
    "mocked_ldap",
)
def test_transform_seq_repo_activities_to_extracted_activities(
    extracted_primary_source_seq_repo: ExtractedPrimarySource,
    seq_repo_latest_sources: dict[str, SeqRepoSource],
    seq_repo_activity: dict[str, Any],
    seq_repo_source_resolved_project_coordinators: list[LDAPPersonWithQuery],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    project_coordinators_merged_ids_by_query_string: dict[
        str, list[MergedPersonIdentifier]
    ],
) -> None:
    expected = {
        "hadPrimarySource": str(extracted_primary_source_seq_repo.stableTargetId),
        "identifierInPrimarySource": "TEST-ID",
        "contact": [
            str(project_coordinators_merged_ids_by_query_string["max"][0]),
            str(project_coordinators_merged_ids_by_query_string["mustermann"][0]),
        ],
        "involvedPerson": [
            str(project_coordinators_merged_ids_by_query_string["max"][0]),
            str(project_coordinators_merged_ids_by_query_string["mustermann"][0]),
        ],
        "responsibleUnit": [str(unit_stable_target_ids_by_synonym["FG99"])],
        "theme": [
            "https://mex.rki.de/item/theme-11",
            "https://mex.rki.de/item/theme-23",
        ],
        "title": [{"value": "FG99-ABC-123", "language": TextLanguage.DE}],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }
    extracted_mex_activities = list(
        transform_seq_repo_activities_to_extracted_activities(
            seq_repo_latest_sources,
            seq_repo_activity,
            seq_repo_source_resolved_project_coordinators,
            unit_stable_target_ids_by_synonym,
            project_coordinators_merged_ids_by_query_string,
            extracted_primary_source_seq_repo,
        )
    )
    assert (
        extracted_mex_activities[0].model_dump(exclude_none=True, exclude_defaults=True)
        == expected
    )


def test_transform_seq_repo_resource_to_extracted_resource(
    extracted_primary_source_seq_repo: ExtractedPrimarySource,
    seq_repo_latest_sources: dict[str, SeqRepoSource],
    extracted_mex_activities_dict: dict[str, ExtractedActivity],
    seq_repo_resource: dict[str, Any],
    extracted_mex_access_platform: ExtractedAccessPlatform,
    seq_repo_source_resolved_project_coordinators: list[LDAPPersonWithQuery],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    project_coordinators_merged_ids_by_query_string: dict[
        str, list[MergedPersonIdentifier]
    ],
    extracted_organization_rki: ExtractedOrganization,
) -> None:
    activity = extracted_mex_activities_dict["TEST-ID"]
    expected_resource = {
        "accessPlatform": [str(extracted_mex_access_platform.stableTargetId)],
        "hadPrimarySource": str(extracted_primary_source_seq_repo.stableTargetId),
        "identifierInPrimarySource": "test-sample-id.TEST",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "accrualPeriodicity": "https://mex.rki.de/item/frequency-15",
        "anonymizationPseudonymization": [
            "https://mex.rki.de/item/anonymization-pseudonymization-2"
        ],
        "contact": [
            str(project_coordinators_merged_ids_by_query_string["max"][0]),
            str(project_coordinators_merged_ids_by_query_string["mustermann"][0]),
        ],
        "contributingUnit": [str(unit_stable_target_ids_by_synonym["FG99"])],
        "created": YearMonthDay("2023-08-07"),
        "instrumentToolOrApparatus": [{"value": "TEST"}],
        "keyword": [
            {
                "value": "fastc",
                "language": TextLanguage.DE,
            },
            {
                "value": "fastd",
                "language": TextLanguage.DE,
            },
            {
                "value": "Severe acute respiratory syndrome coronavirus 2",
                "language": TextLanguage.EN,
            },
        ],
        "method": [
            {"value": "Next-Generation Sequencing", "language": TextLanguage.DE},
            {"value": "NGS", "language": TextLanguage.DE},
        ],
        "publisher": [str(extracted_organization_rki.stableTargetId)],
        "resourceCreationMethod": [
            "https://mex.rki.de/item/resource-creation-method-4"
        ],
        "resourceTypeGeneral": ["https://mex.rki.de/item/resource-type-general-13"],
        "resourceTypeSpecific": [
            {"value": "Sequencing Data", "language": TextLanguage.DE},
            {"value": "Sequenzdaten", "language": TextLanguage.DE},
        ],
        "rights": [{"value": "Example content", "language": TextLanguage.DE}],
        "stateOfDataProcessing": ["https://mex.rki.de/item/data-processing-state-1"],
        "theme": [
            "https://mex.rki.de/item/theme-11",
            "https://mex.rki.de/item/theme-23",
        ],
        "title": [
            {
                "value": "FG99-ABC-123 sample test-customer-name-1",
                "language": TextLanguage.EN,
            }
        ],
        "unitInCharge": [
            str(unit_stable_target_ids_by_synonym["FG99"]),
        ],
        "wasGeneratedBy": str(activity.stableTargetId),
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }

    mex_resources = transform_seq_repo_resource_to_extracted_resource(
        seq_repo_latest_sources,
        extracted_mex_activities_dict,
        extracted_mex_access_platform,
        seq_repo_resource,
        seq_repo_source_resolved_project_coordinators,
        unit_stable_target_ids_by_synonym,
        project_coordinators_merged_ids_by_query_string,
        extracted_organization_rki,
        extracted_primary_source_seq_repo,
    )

    assert (
        mex_resources[0].model_dump(exclude_none=True, exclude_defaults=True)
        == expected_resource
    )


def test_transform_seq_repo_access_platform_to_extracted_access_platform(
    extracted_primary_source_seq_repo: ExtractedPrimarySource,
    seq_repo_access_platform: dict[str, Any],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> None:
    expected = {
        "hadPrimarySource": extracted_primary_source_seq_repo.stableTargetId,
        "identifierInPrimarySource": "https://dummy.url.com/",
        "alternativeTitle": [{"value": "SeqRepo"}],
        "contact": [str(unit_stable_target_ids_by_synonym["FG99"])],
        "description": [
            {
                "value": "This is just a sample description, don't read it.",
                "language": TextLanguage.EN,
            }
        ],
        "endpointType": "https://mex.rki.de/item/api-type-1",
        "landingPage": [{"url": "https://dummy.url.com/"}],
        "technicalAccessibility": "https://mex.rki.de/item/technical-accessibility-1",
        "title": [{"value": "Sequence Data Repository"}],
        "unitInCharge": [str(unit_stable_target_ids_by_synonym["FG99"])],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }

    extracted_mex_access_platform = (
        transform_seq_repo_access_platform_to_extracted_access_platform(
            seq_repo_access_platform,
            unit_stable_target_ids_by_synonym,
            extracted_primary_source_seq_repo,
        )
    )

    assert (
        extracted_mex_access_platform.model_dump(
            exclude_none=True, exclude_defaults=True
        )
        == expected
    )
