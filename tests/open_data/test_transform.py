import pytest

from mex.common.models import (
    ConsentMapping,
    DistributionMapping,
    ExtractedContactPoint,
    ExtractedDistribution,
    ExtractedOrganization,
    ExtractedPerson,
    ExtractedPrimarySource,
    ResourceMapping,
)
from mex.common.organigram.extract import extract_organigram_units
from mex.common.organigram.transform import (
    transform_organigram_units_to_organizational_units,
)
from mex.common.testing import Joker
from mex.common.types import (
    Identifier,
    TextLanguage,
)
from mex.extractors.open_data.models.source import (
    MexPersonAndCreationDate,
    OpenDataParentResource,
    OpenDataResourceVersion,
)
from mex.extractors.open_data.transform import (
    transform_open_data_distributions,
    transform_open_data_parent_resource_to_mex_resource,
    transform_open_data_person_to_mex_consent,
    transform_open_data_persons,
)


@pytest.mark.usefixtures("mocked_open_data", "mocked_ldap")
def test_transform_open_data_persons(
    mocked_open_data_resource_version: OpenDataResourceVersion,
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    extracted_organigram_units = transform_organigram_units_to_organizational_units(
        extract_organigram_units(), extracted_primary_sources["organigram"]
    )
    results = transform_open_data_persons(
        mocked_open_data_resource_version,
        extracted_primary_sources["ldap"],
        extracted_organigram_units,
    )

    assert results == {
        "Muster, Maxi": MexPersonAndCreationDate(
            mex_person=ExtractedPerson(
                hadPrimarySource=extracted_primary_sources["ldap"].stableTargetId,
                identifierInPrimarySource="00000000-0000-4000-8000-000000000001",
                email=["test_person@email.de"],
                familyName=["Resolved"],
                fullName=["Resolved, Roland"],
                givenName=["Roland"],
                memberOf=["hIiJpZXVppHvoyeP0QtAoS"],
            ),
            created="2021-01-01T01:01:01.111111+00:00",
        )
    }


@pytest.mark.usefixtures("mocked_open_data")
def test_transform_open_data_distributions(
    mocked_open_data_parent_resource: OpenDataParentResource,
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    mocked_open_data_distribution_mapping: DistributionMapping,
) -> None:
    mex_distribution = list(
        transform_open_data_distributions(
            mocked_open_data_parent_resource,
            extracted_primary_sources["open-data"],
            mocked_open_data_distribution_mapping,
        )
    )

    assert mex_distribution[0].model_dump(exclude_none=True, exclude_defaults=True) == {
        "hadPrimarySource": "bEwCy4xNTx9gCJr9aJ7LM",
        "accessURL": [{"url": "10.3456/zenodo.7890"}],
        "identifierInPrimarySource": "file_test_id",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-1",
        "issued": "2021-01-01T01:01:01Z",
        "license": "https://mex.rki.de/item/license-1",
        "title": [{"value": "some text"}],
        "downloadURL": [{"url": "www.efg.hi"}],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }


@pytest.mark.usefixtures("mocked_open_data")
def test_transform_open_data_person_to_mex_consent(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    mocked_open_data_persons: list[ExtractedPerson],
    mocked_open_data_consent_mapping: ConsentMapping,
) -> None:
    mocked_open_data_persons_and_creation_date = {
        "Muster, Maxi": MexPersonAndCreationDate(
            mex_person=ExtractedPerson(
                hadPrimarySource=Identifier.generate(seed=42),
                identifierInPrimarySource="test_id",
            ),
            created="2021-01-01T01:01:01.111111+00:00",
        )
    }
    mex_consent_result = list(
        transform_open_data_person_to_mex_consent(
            extracted_primary_sources["open-data"],
            mocked_open_data_persons,
            mocked_open_data_persons_and_creation_date,
            mocked_open_data_consent_mapping,
        )
    )

    assert mex_consent_result[0].model_dump(
        exclude_none=True, exclude_defaults=True
    ) == {
        "hadPrimarySource": extracted_primary_sources["open-data"].stableTargetId,
        "identifierInPrimarySource": f"{mocked_open_data_persons[0].stableTargetId}_consent",
        "hasConsentStatus": "https://mex.rki.de/item/consent-status-2",
        "hasDataSubject": str(mocked_open_data_persons[0].stableTargetId),
        "isIndicatedAtTime": "2021-01-01T01:01:01Z",
        "hasConsentType": "https://mex.rki.de/item/consent-type-1",
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }


@pytest.mark.usefixtures("mocked_ldap", "mocked_open_data")
def test_transform_open_data_parent_resource_to_mex_resource(  # noqa: PLR0913
    mocked_open_data_parent_resource: OpenDataParentResource,
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    mocked_open_data_persons: list[ExtractedPerson],
    mocked_open_data_parent_resource_mapping: ResourceMapping,
    extracted_organization_rki: ExtractedOrganization,
    mocked_open_data_contact_point: ExtractedContactPoint,
    mocked_open_data_distribution: list[ExtractedDistribution],
) -> None:
    unit_stable_target_ids_by_synonym = {
        "C1": Identifier.generate(seed=999),
        "XY": Identifier.generate(seed=959),
    }

    mex_sources = list(
        transform_open_data_parent_resource_to_mex_resource(
            mocked_open_data_parent_resource,
            extracted_primary_sources["open-data"],
            mocked_open_data_persons,
            unit_stable_target_ids_by_synonym,
            mocked_open_data_distribution,
            mocked_open_data_parent_resource_mapping,
            extracted_organization_rki,
            mocked_open_data_contact_point,
        )
    )

    assert len(mex_sources) == 1
    assert mex_sources[0].model_dump(exclude_none=True, exclude_defaults=True) == {
        "hadPrimarySource": str(extracted_primary_sources["open-data"].stableTargetId),
        "identifierInPrimarySource": "Eins",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-1",
        "created": "2021",
        "hasPersonalData": "https://mex.rki.de/item/personal-data-2",
        "license": "https://mex.rki.de/item/license-1",
        "contact": [str(mocked_open_data_contact_point[0].stableTargetId)],
        "theme": ["https://mex.rki.de/item/theme-1"],
        "title": [{"value": "Dumdidumdidum"}],
        "unitInCharge": [str(Identifier.generate(seed=999))],
        "anonymizationPseudonymization": [
            "https://mex.rki.de/item/anonymization-pseudonymization-1"
        ],
        "contributor": [str(mocked_open_data_persons[0].stableTargetId)],
        "description": [
            {"language": TextLanguage.EN, "value": "Test1 <a href='test/2'>test3</a>"}
        ],
        "doi": "https://doi.org/10.3456/zenodo.7890",
        "distribution": [str(mocked_open_data_distribution[0].stableTargetId)],
        "publisher": [str(extracted_organization_rki.stableTargetId)],
        "resourceTypeGeneral": ["https://mex.rki.de/item/resource-type-general-14"],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }
