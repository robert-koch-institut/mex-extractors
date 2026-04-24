from typing import TYPE_CHECKING

import pytest

from mex.extractors.seq_repo.transform import (
    transform_seq_repo_access_platform_to_extracted_access_platform,
    transform_seq_repo_activities_to_extracted_activities,
    transform_seq_repo_resource_to_extracted_resource,
)

if TYPE_CHECKING:
    from mex.common.models import (
        AccessPlatformMapping,
        ActivityMapping,
        ExtractedAccessPlatform,
        ExtractedActivity,
        ExtractedOrganization,
        ResourceMapping,
    )
    from mex.common.types import MergedPersonIdentifier
    from mex.extractors.seq_repo.model import SeqRepoSource


@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_transform_seq_repo_activities_to_extracted_activities(
    seq_repo_latest_sources: dict[str, SeqRepoSource],
    seq_repo_activity: ActivityMapping,
    seq_repo_merged_person_ids_by_name: dict[str, MergedPersonIdentifier],
) -> None:
    expected = {
        "hadPrimarySource": "gFhkyRIWA7LDeKmKz9a3K",
        "identifierInPrimarySource": "TEST-ID",
        "contact": [
            "eXA2Qj5pKmI7HXIgcVqCfz",
            "cpKNwpoZTQ4GpIzBgO8DMx",
            "c2Yd8aNoLKIf7u6ubTUuc3",
        ],
        "responsibleUnit": ["hIiJpZXVppHvoyeP0QtAoS"],
        "title": [{"value": "FG99-ABC-123", "language": "de"}],
        "involvedPerson": [
            "eXA2Qj5pKmI7HXIgcVqCfz",
            "cpKNwpoZTQ4GpIzBgO8DMx",
            "c2Yd8aNoLKIf7u6ubTUuc3",
        ],
        "theme": [
            "https://mex.rki.de/item/theme-11",
            "https://mex.rki.de/item/theme-23",
        ],
        "identifier": "egRPPkE5jnd2jOgr4hosz1",
        "stableTargetId": "fPqFxu76FLQjVxUDSJpb0z",
    }
    extracted_mex_activities = transform_seq_repo_activities_to_extracted_activities(
        seq_repo_latest_sources,
        seq_repo_activity,
        seq_repo_merged_person_ids_by_name,
    )
    assert extracted_mex_activities
    assert (
        extracted_mex_activities[0].model_dump(exclude_none=True, exclude_defaults=True)
        == expected
    )


@pytest.mark.usefixtures("mocked_wikidata", "mocked_ldap")
def test_transform_seq_repo_resource_to_extracted_resource(  # noqa: PLR0913
    seq_repo_latest_sources: dict[str, SeqRepoSource],
    extracted_mex_activities_dict: dict[str, ExtractedActivity],
    seq_repo_resource: ResourceMapping,
    extracted_mex_access_platform: ExtractedAccessPlatform,
    seq_repo_merged_person_ids_by_name: dict[str, MergedPersonIdentifier],
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
            "eXA2Qj5pKmI7HXIgcVqCfz",
            "cpKNwpoZTQ4GpIzBgO8DMx",
            "c2Yd8aNoLKIf7u6ubTUuc3",
        ],
        "theme": [
            "https://mex.rki.de/item/theme-11",
            "https://mex.rki.de/item/theme-23",
        ],
        "title": [
            {"value": "FG99-ABC-123 sample test-customer-name-1", "language": "en"}
        ],
        "unitInCharge": ["hIiJpZXVppHvoyeP0QtAoS"],
        "accessPlatform": ["gLB9vC2lPMy5rCmuot99xu"],
        "anonymizationPseudonymization": [
            "https://mex.rki.de/item/anonymization-pseudonymization-2"
        ],
        "description": [
            {"value": "Testbeschreibung", "language": "de"},
            {"value": "test description", "language": "en"},
        ],
        "instrumentToolOrApparatus": [{"value": "TEST"}],
        "keyword": [
            {"value": "fastc", "language": "de"},
            {"value": "fastd", "language": "de"},
            {"value": "virus XYZ"},
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
        seq_repo_merged_person_ids_by_name,
        extracted_organization_rki,
    )

    assert (
        mex_resources[0].model_dump(exclude_none=True, exclude_defaults=True)
        == expected_resource
    )
    assert mex_resources[1].contributingUnit == []


@pytest.mark.usefixtures("mocked_wikidata")
def test_transform_seq_repo_access_platform_to_extracted_access_platform(
    seq_repo_access_platform: AccessPlatformMapping,
) -> None:
    expected = {
        "hadPrimarySource": "gFhkyRIWA7LDeKmKz9a3K",
        "identifierInPrimarySource": "https://dummy.url.com/",
        "technicalAccessibility": "https://mex.rki.de/item/technical-accessibility-1",
        "endpointType": "https://mex.rki.de/item/api-type-1",
        "alternativeTitle": [{"value": "SeqRepo"}],
        "contact": ["6rqNvZSApUHlz8GkkVP48"],
        "description": [
            {
                "value": "This is just a sample description, don't read it.",
                "language": "en",
            }
        ],
        "landingPage": [{"url": "https://dummy.url.com/"}],
        "title": [{"value": "Sequence Data Repository"}],
        "unitInCharge": ["6rqNvZSApUHlz8GkkVP48"],
        "identifier": "1WygpViC8MZc8YueccIPh",
        "stableTargetId": "gLB9vC2lPMy5rCmuot99xu",
    }

    extracted_mex_access_platform = (
        transform_seq_repo_access_platform_to_extracted_access_platform(
            seq_repo_access_platform,
        )
    )

    assert (
        extracted_mex_access_platform.model_dump(
            exclude_none=True, exclude_defaults=True
        )
        == expected
    )
