import pytest

from mex.common.ldap.models import LDAPPersonWithQuery
from mex.common.models import (
    AccessPlatformMapping,
    ActivityMapping,
    ExtractedAccessPlatform,
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedPrimarySource,
    ResourceMapping,
)
from mex.common.testing import Joker
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedPersonIdentifier,
    TextLanguage,
)
from mex.extractors.seq_repo.model import SeqRepoSource
from mex.extractors.seq_repo.transform import (
    transform_seq_repo_access_platform_to_extracted_access_platform,
    transform_seq_repo_activities_to_extracted_activities,
    transform_seq_repo_resource_to_extracted_resource,
)


@pytest.mark.usefixtures("mocked_ldap")
def test_transform_seq_repo_activities_to_extracted_activities(  # noqa: PLR0913
    seq_repo_extracted_primary_source: ExtractedPrimarySource,
    seq_repo_latest_sources: dict[str, SeqRepoSource],
    seq_repo_activity: ActivityMapping,
    seq_repo_source_resolved_project_coordinators: list[LDAPPersonWithQuery],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    seq_repo_project_coordinators_merged_ids_by_query_string: dict[
        str, list[MergedPersonIdentifier]
    ],
) -> None:
    expected = {
        "hadPrimarySource": "gFhkyRIWA7LDeKmKz9a3K",
        "identifierInPrimarySource": "TEST-ID",
        "contact": [
            "d6Lni0XPiEQM5jILEBOYxO",
            "e0Rxxm9WvnMqPLZ44UduNx",
            "d6Lni0XPiEQM5jILEBOYxO",
        ],
        "responsibleUnit": ["e4fyMCGjCeQNSvAMNHcBhK"],
        "title": [{"value": "FG99-ABC-123", "language": "de"}],
        "involvedPerson": [
            "d6Lni0XPiEQM5jILEBOYxO",
            "e0Rxxm9WvnMqPLZ44UduNx",
            "d6Lni0XPiEQM5jILEBOYxO",
        ],
        "theme": [
            "https://mex.rki.de/item/theme-11",
            "https://mex.rki.de/item/theme-23",
        ],
        "identifier": "egRPPkE5jnd2jOgr4hosz1",
        "stableTargetId": "fPqFxu76FLQjVxUDSJpb0z",
    }
    extracted_mex_activities = list(
        transform_seq_repo_activities_to_extracted_activities(
            seq_repo_latest_sources,
            seq_repo_activity,
            seq_repo_source_resolved_project_coordinators,
            unit_stable_target_ids_by_synonym,
            seq_repo_project_coordinators_merged_ids_by_query_string,
            seq_repo_extracted_primary_source,
        )
    )
    assert (
        extracted_mex_activities[0].model_dump(exclude_none=True, exclude_defaults=True)
        == expected
    )


def test_transform_seq_repo_resource_to_extracted_resource(  # noqa: PLR0913
    seq_repo_extracted_primary_source: ExtractedPrimarySource,
    seq_repo_latest_sources: dict[str, SeqRepoSource],
    extracted_mex_activities_dict: dict[str, ExtractedActivity],
    seq_repo_resource: ResourceMapping,
    extracted_mex_access_platform: ExtractedAccessPlatform,
    seq_repo_source_resolved_project_coordinators: list[LDAPPersonWithQuery],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    seq_repo_project_coordinators_merged_ids_by_query_string: dict[
        str, list[MergedPersonIdentifier]
    ],
    extracted_organization_rki: ExtractedOrganization,
) -> None:
    expected_resource = {
        "hadPrimarySource": "gFhkyRIWA7LDeKmKz9a3K",
        "identifierInPrimarySource": "test-sample-id.TEST",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "accrualPeriodicity": "https://mex.rki.de/item/frequency-15",
        "created": "2023-08-07",
        "wasGeneratedBy": "fPqFxu76FLQjVxUDSJpb0z",
        "contact": [
            "d6Lni0XPiEQM5jILEBOYxO",
            "e0Rxxm9WvnMqPLZ44UduNx",
            "d6Lni0XPiEQM5jILEBOYxO",
        ],
        "theme": [
            "https://mex.rki.de/item/theme-11",
            "https://mex.rki.de/item/theme-23",
        ],
        "title": [
            {"value": "FG99-ABC-123 sample test-customer-name-1", "language": "en"}
        ],
        "unitInCharge": ["e4fyMCGjCeQNSvAMNHcBhK"],
        "accessPlatform": ["gLB9vC2lPMy5rCmuot99xu"],
        "anonymizationPseudonymization": [
            "https://mex.rki.de/item/anonymization-pseudonymization-2"
        ],
        "contributingUnit": ["e4fyMCGjCeQNSvAMNHcBhK"],
        "description": [
            {"value": "Testbeschreibung", "language": "de"},
            {"value": "test description", "language": "en"},
        ],
        "instrumentToolOrApparatus": [{"value": "TEST"}],
        "keyword": [
            {"value": "fastc", "language": "de"},
            {"value": "fastd", "language": "de"},
            {
                "value": "Severe acute respiratory syndrome coronavirus 2",
                "language": "en",
            },
        ],
        "method": [
            {"value": "Next-Generation Sequencing", "language": "de"},
            {"value": "NGS", "language": "de"},
        ],
        "publisher": ["fxIeF3TWocUZoMGmBftJ6x"],
        "resourceCreationMethod": [
            "https://mex.rki.de/item/resource-creation-method-4"
        ],
        "resourceTypeGeneral": ["https://mex.rki.de/item/resource-type-general-13"],
        "resourceTypeSpecific": [
            {"value": "Sequencing Data", "language": "de"},
            {"value": "Sequenzdaten", "language": "de"},
        ],
        "rights": [{"value": "Example content", "language": "de"}],
        "stateOfDataProcessing": ["https://mex.rki.de/item/data-processing-state-1"],
        "identifier": "cYmhNKAP5uCRwuspCYccyc",
        "stableTargetId": "cca31cXT1dWCdf3hUeO1PR",
    }

    mex_resources = transform_seq_repo_resource_to_extracted_resource(
        seq_repo_latest_sources,
        extracted_mex_activities_dict,
        extracted_mex_access_platform,
        seq_repo_resource,
        seq_repo_source_resolved_project_coordinators,
        unit_stable_target_ids_by_synonym,
        seq_repo_project_coordinators_merged_ids_by_query_string,
        extracted_organization_rki,
        seq_repo_extracted_primary_source,
    )

    assert (
        mex_resources[0].model_dump(exclude_none=True, exclude_defaults=True)
        == expected_resource
    )


def test_transform_seq_repo_access_platform_to_extracted_access_platform(
    seq_repo_extracted_primary_source: ExtractedPrimarySource,
    seq_repo_access_platform: AccessPlatformMapping,
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> None:
    expected = {
        "hadPrimarySource": seq_repo_extracted_primary_source.stableTargetId,
        "identifierInPrimarySource": "https://dummy.url.com/",
        "alternativeTitle": [{"language": "es", "value": "SeqRepo"}],
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
        "title": [{"language": "es", "value": "Sequence Data Repository"}],
        "unitInCharge": [str(unit_stable_target_ids_by_synonym["FG99"])],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }

    extracted_mex_access_platform = (
        transform_seq_repo_access_platform_to_extracted_access_platform(
            seq_repo_access_platform,
            unit_stable_target_ids_by_synonym,
            seq_repo_extracted_primary_source,
        )
    )

    assert (
        extracted_mex_access_platform.model_dump(
            exclude_none=True, exclude_defaults=True
        )
        == expected
    )
