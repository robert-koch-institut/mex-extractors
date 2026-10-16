from typing import TYPE_CHECKING

import pytest

from mex.common.types import TextLanguage
from mex.extractors.organigram.helpers import get_unit_merged_id_by_synonym
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
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
        ExtractedOrganizationalUnit,
        ExtractedPerson,
        ExtractedResourceSeries,
        ResourceMapping,
        ResourceSeriesMapping,
    )
    from mex.extractors.seq_repo.model import SeqRepoSource


@pytest.mark.usefixtures("mocked_wikidata", "mocked_ldap")
def test_transform_seq_repo_resource_to_extracted_resource_series(
    seq_repo_sources: list[SeqRepoSource],
    seq_repo_resource_series_mapping: ResourceSeriesMapping,
    extracted_access_platform: ExtractedAccessPlatform,
    extracted_organization_rki: ExtractedOrganization,
    mocked_units_by_identifier_in_primary_source: dict[
        str, ExtractedOrganizationalUnit
    ],
) -> None:
    resource_series = transform_seq_repo_resource_to_extracted_resource_series(
        seq_repo_resource_series_mapping,
        seq_repo_sources,
        extracted_access_platform,
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

    assert test_id_series.model_dump(exclude_none=True, exclude_defaults=True) == {
        "accessPlatform": [extracted_access_platform.stableTargetId],
        "contact": [
            mocked_units_by_identifier_in_primary_source["fg99"].stableTargetId,
            mocked_units_by_identifier_in_primary_source["parent-unit"].stableTargetId,
        ],
        "description": [
            {
                "value": "Some text with project name 'FG99-ABC-123' (additional 'FG99-ABC-321', 'SKIPPED BECAUSE LIMS-SAMPLE-ID ALREADY EXISTS').",
                "language": TextLanguage.EN,
            }
        ],
        "end": ["2023-08-07"],
        "hadPrimarySource": get_extracted_primary_source_id_by_name("seq-repo"),
        "identifier": "egRPPkE5jnd2jOgr4hosz1",
        "identifierInPrimarySource": "TEST-ID",
        "keyword": [
            {"value": "Key Word", "language": TextLanguage.EN},
            {"value": "Lab rat"},
            {"value": "TEST"},
            {"value": "virus XYZ"},
        ],
        "publisher": [extracted_organization_rki.stableTargetId],
        "stableTargetId": "fPqFxu76FLQjVxUDSJpb0z",
        "start": ["2023-08-07"],
        "title": [
            {"value": "FG99-ABC-123", "language": TextLanguage.DE},
            {"value": "FG99-ABC-321", "language": TextLanguage.DE},
            {
                "value": "SKIPPED BECAUSE LIMS-SAMPLE-ID ALREADY EXISTS",
                "language": TextLanguage.DE,
            },
        ],
    }

    test_id_2_series = resource_series_by_project_id["TEST-ID-2"]

    assert {title.value for title in test_id_2_series.title} == {"FG99-ABC-789"}
    assert {keyword.value for keyword in test_id_2_series.keyword} == {
        "Key Word",
        "guniea pig",
        "TEST-2",
    }


@pytest.mark.usefixtures("mocked_wikidata", "mocked_ldap")
def test_transform_seq_repo_resource_to_extracted_resource(  # noqa: PLR0913, PLR0917
    seq_repo_sources: list[SeqRepoSource],
    seq_repo_resource_mapping: ResourceMapping,
    extracted_access_platform: ExtractedAccessPlatform,
    extracted_resource_series: list[ExtractedResourceSeries],
    extracted_organization_rki: ExtractedOrganization,
    mocked_units_by_identifier_in_primary_source: dict[
        str, ExtractedOrganizationalUnit
    ],
    roland_resolved: ExtractedPerson,
    frieda_fictitious: ExtractedPerson,
) -> None:
    expected_resource = {
        "hadPrimarySource": get_extracted_primary_source_id_by_name("seq-repo"),
        "identifierInPrimarySource": "test-sample-id",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "accrualPeriodicity": "https://mex.rki.de/item/frequency-15",
        "start": ["2023-08-07"],
        "modified": "2023-08-07",
        "contact": [
            mocked_units_by_identifier_in_primary_source["fg99"].stableTargetId,
            mocked_units_by_identifier_in_primary_source["parent-unit"].stableTargetId,
        ],
        "theme": [
            "https://mex.rki.de/item/theme-11",
            "https://mex.rki.de/item/theme-23",
        ],
        "title": [{"value": "LIMS Sample ID test-sample-id (virus XYZ)"}],
        "unitInCharge": [
            mocked_units_by_identifier_in_primary_source["fg99"].stableTargetId,
            mocked_units_by_identifier_in_primary_source["parent-unit"].stableTargetId,
        ],
        "accessPlatform": [extracted_access_platform.stableTargetId],
        "anonymizationPseudonymization": [
            "https://mex.rki.de/item/anonymization-pseudonymization-2"
        ],
        "contributingUnit": [
            mocked_units_by_identifier_in_primary_source["fg99"].stableTargetId
        ],
        "contributor": [
            frieda_fictitious.stableTargetId,
            roland_resolved.stableTargetId,
        ],
        "description": [
            {"value": "Testbeschreibung", "language": TextLanguage.DE},
            {"value": "test description", "language": TextLanguage.EN},
        ],
        "healthCategory": ["https://mex.rki.de/item/health-category-1"],
        "keyword": [
            {"value": "fastc", "language": TextLanguage.DE},
            {"value": "fastd", "language": TextLanguage.DE},
            {"value": "virus XYZ"},
            {"value": "TEST"},
        ],
        "inSeries": [extracted_resource_series[0].stableTargetId],
        "publisher": [extracted_organization_rki.stableTargetId],
        "qualityInformation": [
            {"value": "Basepairs: 1", "language": TextLanguage.EN},
            {"value": "Reads: 2", "language": TextLanguage.EN},
        ],
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
        "identifier": "bTEDLdzkS7wXaw3ZzP3JdC",
        "stableTargetId": "c1KjL7zbtcibP9bmxpo8fr",
    }
    mex_resources = transform_seq_repo_resource_to_extracted_resource(
        seq_repo_sources,
        extracted_access_platform,
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
        "hadPrimarySource": get_extracted_primary_source_id_by_name("seq-repo"),
        "identifierInPrimarySource": "https://dummy.url.com/",
        "technicalAccessibility": "https://mex.rki.de/item/technical-accessibility-1",
        "endpointType": "https://mex.rki.de/item/api-type-1",
        "alternativeTitle": [{"value": "SeqRepo"}],
        "contact": get_unit_merged_id_by_synonym("FG99"),
        "description": [
            {
                "value": "This is just a sample description, don't read it.",
                "language": TextLanguage.EN,
            }
        ],
        "landingPage": [{"url": "https://dummy.url.com/"}],
        "title": [{"value": "Sequence Data Repository"}],
        "unitInCharge": get_unit_merged_id_by_synonym("FG99"),
        "identifier": "1WygpViC8MZc8YueccIPh",
        "stableTargetId": "gLB9vC2lPMy5rCmuot99xu",
    }

    extracted_access_platform = (
        transform_seq_repo_access_platform_to_extracted_access_platform(
            seq_repo_access_platform_mapping,
        )
    )

    assert (
        extracted_access_platform.model_dump(exclude_none=True, exclude_defaults=True)
        == expected
    )
