from typing import TYPE_CHECKING

import pytest

from mex.common.testing import Joker
from mex.common.types import (
    MergedPersonIdentifier,
    TextLanguage,
)
from mex.extractors.organigram.helpers import get_unit_merged_id_by_synonym
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.seq_repo.transform import (
    transform_seq_repo_access_platform_to_extracted_access_platform,
    transform_seq_repo_activities_to_extracted_activities,
    transform_seq_repo_resource_to_extracted_resource,
)

if TYPE_CHECKING:
    from mex.common.ldap.models import LDAPPersonWithQuery
    from mex.common.models import (
        AccessPlatformMapping,
        ActivityMapping,
        ExtractedAccessPlatform,
        ExtractedActivity,
        ExtractedOrganization,
        ResourceMapping,
    )
    from mex.extractors.seq_repo.model import SeqRepoSource


@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_transform_seq_repo_activities_to_extracted_activities(
    seq_repo_sources: list[SeqRepoSource],
    seq_repo_activity: ActivityMapping,
    seq_repo_ldap_persons_with_query: list[LDAPPersonWithQuery],
    seq_repo_merged_person_ids_by_query_string: dict[str, list[MergedPersonIdentifier]],
) -> None:
    expected = {
        "hadPrimarySource": "gFhkyRIWA7LDeKmKz9a3K",
        "identifierInPrimarySource": "TEST-ID",
        "contact": [
            "c2Yd8aNoLKIf7u6ubTUuc3",
            "eXA2Qj5pKmI7HXIgcVqCfz",
        ],
        "responsibleUnit": ["cjna2jitPngp6yIV63cdi9"],
        "title": [{"value": "FG99-ABC-123", "language": TextLanguage.DE}],
        "involvedPerson": [
            "c2Yd8aNoLKIf7u6ubTUuc3",
            "eXA2Qj5pKmI7HXIgcVqCfz",
        ],
        "theme": [
            "https://mex.rki.de/item/theme-11",
            "https://mex.rki.de/item/theme-23",
        ],
        "identifier": "egRPPkE5jnd2jOgr4hosz1",
        "stableTargetId": "fPqFxu76FLQjVxUDSJpb0z",
    }
    extracted_mex_activities = transform_seq_repo_activities_to_extracted_activities(
        seq_repo_sources,
        seq_repo_activity,
        seq_repo_ldap_persons_with_query,
        seq_repo_merged_person_ids_by_query_string,
    )
    assert extracted_mex_activities
    assert (
        extracted_mex_activities[0].model_dump(exclude_none=True, exclude_defaults=True)
        == expected
    )


@pytest.mark.usefixtures("mocked_wikidata")
def test_transform_seq_repo_resource_to_extracted_resource(  # noqa: PLR0913
    seq_repo_sources: list[SeqRepoSource],
    extracted_mex_activities_dict: dict[str, ExtractedActivity],
    seq_repo_resource: ResourceMapping,
    extracted_mex_access_platform: ExtractedAccessPlatform,
    seq_repo_ldap_persons_with_query: list[LDAPPersonWithQuery],
    seq_repo_merged_person_ids_by_query_string: dict[str, list[MergedPersonIdentifier]],
    extracted_organization_rki: ExtractedOrganization,
) -> None:
    expected_resource = {
        "hadPrimarySource": "gFhkyRIWA7LDeKmKz9a3K",
        "identifierInPrimarySource": "test-sample-id",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "accrualPeriodicity": "https://mex.rki.de/item/frequency-15",
        "created": "2023-08-07",
        "modified": "2023-08-07",
        "sizeOfDataBasis": "Basepairs: 1, Reads: 2",
        "wasGeneratedBy": "fPqFxu76FLQjVxUDSJpb0z",
        "contact": ["c2Yd8aNoLKIf7u6ubTUuc3", "eXA2Qj5pKmI7HXIgcVqCfz"],
        "theme": [
            "https://mex.rki.de/item/theme-11",
            "https://mex.rki.de/item/theme-23",
        ],
        "title": [{"value": "LIMS Sample ID test-sample-id (virus XYZ)"}],
        "unitInCharge": ["cjna2jitPngp6yIV63cdi9"],
        "accessPlatform": ["gLB9vC2lPMy5rCmuot99xu"],
        "anonymizationPseudonymization": [
            "https://mex.rki.de/item/anonymization-pseudonymization-2"
        ],
        "contributingUnit": ["cjna2jitPngp6yIV63cdi9"],
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
        "identifier": "bTEDLdzkS7wXaw3ZzP3JdC",
        "stableTargetId": "c1KjL7zbtcibP9bmxpo8fr",
    }

    mex_resources = transform_seq_repo_resource_to_extracted_resource(
        seq_repo_sources,
        extracted_mex_activities_dict,
        extracted_mex_access_platform,
        seq_repo_resource,
        seq_repo_ldap_persons_with_query,
        seq_repo_merged_person_ids_by_query_string,
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
        "hadPrimarySource": get_extracted_primary_source_id_by_name("seq-repo"),
        "identifierInPrimarySource": "https://dummy.url.com/",
        "alternativeTitle": [{"value": "SeqRepo"}],
        "contact": get_unit_merged_id_by_synonym("FG99"),
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
        "unitInCharge": get_unit_merged_id_by_synonym("FG99"),
        "identifier": Joker(),
        "stableTargetId": Joker(),
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
