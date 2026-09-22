from typing import TYPE_CHECKING

import pytest

from mex.extractors.organigram.helpers import get_unit_merged_id_by_synonym
from mex.extractors.seq_repo.transform import (
    transform_seq_repo_access_platform_to_extracted_access_platform,
    transform_seq_repo_resource_to_extracted_resource,
    transform_seq_repo_resource_to_extracted_resource_series,
)

if TYPE_CHECKING:
    from mex.common.models import (
        AccessPlatformMapping,
        ExtractedAccessPlatform,
        ExtractedOrganization,
        ExtractedResourceSeries,
        ResourceMapping,
        ResourceSeriesMapping,
    )
    from mex.extractors.seq_repo.model import SeqRepoSource


@pytest.mark.usefixtures("mocked_wikidata", "mocked_ldap")
def test_transform_seq_repo_resource_to_extracted_resource_series(
    seq_repo_sources: list[SeqRepoSource],
    seq_repo_resource_series_mapping: ResourceSeriesMapping,
    extracted_mex_access_platform: ExtractedAccessPlatform,
    extracted_organization_rki: ExtractedOrganization,
) -> None:
    resource_series = transform_seq_repo_resource_to_extracted_resource_series(
        seq_repo_resource_series_mapping,
        seq_repo_sources,
        extracted_mex_access_platform,
        extracted_organization_rki,
    )

    assert len(resource_series) == 2

    resource_series_by_project_id = {
        item.identifierInPrimarySource: item for item in resource_series
    }

    assert set(resource_series_by_project_id) == {
        "TEST-ID",
        "TEST-ID-2",
    }

    test_id_series = resource_series_by_project_id["TEST-ID"]

    assert {title.value for title in test_id_series.title} == {
        "FG99-ABC-123",
        "FG99-ABC-321",
        "SKIPPED BECAUSE LIMS-SAMPLE-ID ALREADY EXISTS",
    }
    assert test_id_series.accessPlatform == [
        extracted_mex_access_platform.stableTargetId
    ]
    assert test_id_series.publisher == [extracted_organization_rki.stableTargetId]

    assert {keyword.value for keyword in test_id_series.keyword} == {
        "Key Word",
        "virus XYZ",
        "Lab rat",
        "TEST",
    }

    test_id_2_series = resource_series_by_project_id["TEST-ID-2"]

    assert {title.value for title in test_id_2_series.title} == {"FG99-ABC-789"}
    assert {keyword.value for keyword in test_id_2_series.keyword} == {
        "Key Word",
        "guniea pig",
        "TEST-2",
    }


@pytest.mark.usefixtures("mocked_wikidata", "mocked_ldap")
def test_transform_seq_repo_resource_to_extracted_resource(
    seq_repo_sources: list[SeqRepoSource],
    seq_repo_resource_mapping: ResourceMapping,
    extracted_mex_access_platform: ExtractedAccessPlatform,
    extracted_resource_series: list[ExtractedResourceSeries],
    extracted_organization_rki: ExtractedOrganization,
) -> None:
    expected_resource = {
        "hadPrimarySource": "gFhkyRIWA7LDeKmKz9a3K",
        "identifierInPrimarySource": "test-sample-id",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "accrualPeriodicity": "https://mex.rki.de/item/frequency-15",
        "start": ["2023-08-07"],
        "modified": "2023-08-07",
        "contact": ["c2Yd8aNoLKIf7u6ubTUuc3", "eXA2Qj5pKmI7HXIgcVqCfz"],
        "theme": [
            "https://mex.rki.de/item/theme-11",
            "https://mex.rki.de/item/theme-23",
        ],
        "title": [{"value": "LIMS Sample ID test-sample-id (virus XYZ)"}],
        "unitInCharge": ["cjna2jitPngp6yIV63cdi9", "hIiJpZXVppHvoyeP0QtAoS"],
        "accessPlatform": ["gLB9vC2lPMy5rCmuot99xu"],
        "anonymizationPseudonymization": [
            "https://mex.rki.de/item/anonymization-pseudonymization-2"
        ],
        "contributingUnit": ["cjna2jitPngp6yIV63cdi9"],
        "description": [
            {"value": "Testbeschreibung", "language": "de"},
            {"value": "test description", "language": "en"},
        ],
        "healthCategory": ["https://mex.rki.de/item/health-category-1"],
        "keyword": [
            {"value": "fastc", "language": "de"},
            {"value": "fastd", "language": "de"},
            {"value": "virus XYZ"},
            {"value": "TEST"},
        ],
        "inSeries": ["dtxk6Fk8iHe849ArDRywlS"],
        "publisher": ["fxIeF3TWocUZoMGmBftJ6x"],
        "qualityInformation": [
            {"value": "Basepairs: 1", "language": "en"},
            {"value": "Reads: 2", "language": "en"},
        ],
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
        "identifier": "bTEDLdzkS7wXaw3ZzP3JdC",
        "stableTargetId": "c1KjL7zbtcibP9bmxpo8fr",
    }
    mex_resources = transform_seq_repo_resource_to_extracted_resource(
        seq_repo_sources,
        extracted_mex_access_platform,
        extracted_resource_series,
        seq_repo_resource_mapping,
        extracted_organization_rki,
    )

    assert (
        mex_resources[0].model_dump(exclude_none=True, exclude_defaults=True)
        == expected_resource
    )
    assert mex_resources[1].contributingUnit == []


@pytest.mark.usefixtures("mocked_wikidata")
def test_transform_seq_repo_access_platform_to_extracted_access_platform(
    seq_repo_access_platform_mapping: AccessPlatformMapping,
) -> None:
    expected = {
        "hadPrimarySource": "gFhkyRIWA7LDeKmKz9a3K",
        "identifierInPrimarySource": "https://dummy.url.com/",
        "technicalAccessibility": "https://mex.rki.de/item/technical-accessibility-1",
        "endpointType": "https://mex.rki.de/item/api-type-1",
        "alternativeTitle": [{"value": "SeqRepo"}],
        "contact": get_unit_merged_id_by_synonym("FG99"),
        "description": [
            {
                "value": "This is just a sample description, don't read it.",
                "language": "en",
            }
        ],
        "landingPage": [{"url": "https://dummy.url.com/"}],
        "title": [{"value": "Sequence Data Repository"}],
        "unitInCharge": get_unit_merged_id_by_synonym("FG99"),
        "identifier": "1WygpViC8MZc8YueccIPh",
        "stableTargetId": "gLB9vC2lPMy5rCmuot99xu",
    }

    extracted_mex_access_platform = (
        transform_seq_repo_access_platform_to_extracted_access_platform(
            seq_repo_access_platform_mapping,
        )
    )

    assert (
        extracted_mex_access_platform.model_dump(
            exclude_none=True, exclude_defaults=True
        )
        == expected
    )
