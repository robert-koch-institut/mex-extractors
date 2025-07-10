import pytest

from mex.common.models import (
    DistributionMapping,
    ExtractedContactPoint,
    ExtractedDistribution,
    ExtractedOrganization,
    ExtractedOrganizationalUnit,
    ExtractedPerson,
    ExtractedPrimarySource,
    ResourceMapping,
)
from mex.common.testing import Joker
from mex.common.types import (
    Identifier,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    TextLanguage,
)
from mex.extractors.open_data.models.source import (
    OpenDataCreatorsOrContributors,
    OpenDataParentResource,
)
from mex.extractors.open_data.transform import (
    lookup_person_in_ldap_and_transfom,
    transform_open_data_distributions,
    transform_open_data_parent_resource_to_mex_resource,
    transform_open_data_person_affiliations_to_organisations,
    transform_open_data_persons,
    transform_open_data_persons_not_in_ldap,
)


def test_transform_open_data_person_affiliations_to_organisations(
    mocked_open_data_creator_with_processed_affiliation: OpenDataCreatorsOrContributors,
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    results = transform_open_data_person_affiliations_to_organisations(
        [mocked_open_data_creator_with_processed_affiliation],
        extracted_primary_sources["open-data"],
    )
    assert results == {"Universität": Joker()}


def test_transform_open_data_persons_not_in_ldap_and_process_affiliation(
    mocked_open_data_creator_with_processed_affiliation: OpenDataCreatorsOrContributors,
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    extracted_organization_rki: ExtractedOrganization,
) -> None:
    extracted_open_data_organizations = {
        "Universität": MergedOrganizationIdentifier.generate(seed=354)
    }

    results = transform_open_data_persons_not_in_ldap(
        mocked_open_data_creator_with_processed_affiliation,
        extracted_primary_sources["open-data"],
        extracted_organization_rki,
        extracted_open_data_organizations,
    )
    assert results == ExtractedPerson(
        hadPrimarySource=extracted_primary_sources["open-data"].stableTargetId,
        identifierInPrimarySource="Resolved, Roland",
        fullName="Resolved, Roland",
        affiliation=MergedOrganizationIdentifier.generate(seed=354),
        orcidId=[],
    )


def test_transform_open_data_persons_not_in_ldap_and_ignore_affiliation(
    mocked_open_data_creator_with_affiliation_to_ignore: OpenDataCreatorsOrContributors,
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    extracted_organization_rki: ExtractedOrganization,
) -> None:
    extracted_open_data_organizations = {
        "Universität": MergedOrganizationIdentifier.generate(seed=354),
        "RKI": extracted_organization_rki.stableTargetId,
    }
    results = transform_open_data_persons_not_in_ldap(
        mocked_open_data_creator_with_affiliation_to_ignore,
        extracted_primary_sources["open-data"],
        extracted_organization_rki,
        extracted_open_data_organizations,
    )
    assert results == ExtractedPerson(
        hadPrimarySource=extracted_primary_sources["open-data"].stableTargetId,
        identifierInPrimarySource="Muster, Maxi",
        fullName="Muster, Maxi",
        affiliation=None,
        orcidId=[],
    )


@pytest.mark.usefixtures("mocked_ldap")
def test_lookup_person_in_ldap_and_transfom(
    mocked_open_data_creator_with_affiliation_to_ignore: OpenDataCreatorsOrContributors,
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    mocked_units_by_identifier_in_primary_source: dict[
        str, ExtractedOrganizationalUnit
    ],
) -> None:
    results = lookup_person_in_ldap_and_transfom(
        mocked_open_data_creator_with_affiliation_to_ignore,
        extracted_primary_sources["ldap"],
        mocked_units_by_identifier_in_primary_source,
    )
    assert results == ExtractedPerson(
        hadPrimarySource=extracted_primary_sources["ldap"].stableTargetId,
        identifierInPrimarySource="00000000-0000-4000-8000-000000000001",
        affiliation=[],
        email=["test_person@email.de"],
        familyName=["Resolved"],
        fullName=["Resolved, Roland"],
        givenName=["Roland"],
        memberOf="hIiJpZXVppHvoyeP0QtAoS",
        orcidId=[],
        identifier=Joker(),
        stableTargetId=Joker(),
    )


@pytest.mark.usefixtures("mocked_ldap")
def test_transform_open_data_persons(
    mocked_open_data_creator_with_affiliation_to_ignore: OpenDataCreatorsOrContributors,
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    mocked_extracted_organizational_units: list[ExtractedOrganizationalUnit],
    extracted_organization_rki: ExtractedOrganization,
) -> None:
    extracted_open_data_organizations = {
        "Universität": MergedOrganizationIdentifier.generate(seed=354)
    }
    results = transform_open_data_persons(
        [mocked_open_data_creator_with_affiliation_to_ignore],
        extracted_primary_sources["ldap"],
        extracted_primary_sources["open-data"],
        mocked_extracted_organizational_units,
        extracted_organization_rki,
        extracted_open_data_organizations,
    )

    assert results == [
        ExtractedPerson(
            hadPrimarySource=extracted_primary_sources["ldap"].stableTargetId,
            identifierInPrimarySource="00000000-0000-4000-8000-000000000001",
            affiliation=[],
            email=["test_person@email.de"],
            familyName=["Resolved"],
            fullName=["Resolved, Roland"],
            givenName=["Roland"],
            memberOf="hIiJpZXVppHvoyeP0QtAoS",
            orcidId=["https://orcid.org/1234567890"],
            identifier=Joker(),
            stableTargetId=Joker(),
        )
    ]


@pytest.mark.usefixtures("mocked_open_data")
def test_transform_open_data_distributions(
    mocked_open_data_parent_resource: list[OpenDataParentResource],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    mocked_open_data_distribution_mapping: DistributionMapping,
) -> None:
    mex_distribution = transform_open_data_distributions(
        mocked_open_data_parent_resource,
        extracted_primary_sources["open-data"],
        mocked_open_data_distribution_mapping,
    )

    assert mex_distribution[0].model_dump(exclude_none=True, exclude_defaults=True) == {
        "hadPrimarySource": "bEwCy4xNTx9gCJr9aJ7LM",
        "accessURL": [{"url": "https://doi.org/10.3456/zenodo.7890"}],
        "identifierInPrimarySource": "file_test_id",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-1",
        "issued": "2021-01-01T01:01:01Z",
        "license": "https://mex.rki.de/item/license-1",
        "title": [{"value": "some text"}],
        "downloadURL": [{"url": "www.efg.hi"}],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }


@pytest.mark.usefixtures("mocked_ldap", "mocked_open_data")
def test_transform_open_data_parent_resource_to_mex_resource(  # noqa: PLR0913
    mocked_open_data_parent_resource: list[OpenDataParentResource],
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    mocked_open_data_persons: list[ExtractedPerson],
    mocked_open_data_parent_resource_mapping: ResourceMapping,
    extracted_organization_rki: ExtractedOrganization,
    mocked_open_data_contact_point: list[ExtractedContactPoint],
    mocked_open_data_distribution: list[ExtractedDistribution],
) -> None:
    unit_stable_target_ids_by_synonym = {
        "C1": MergedOrganizationalUnitIdentifier.generate(seed=999),
        "XY": MergedOrganizationalUnitIdentifier.generate(seed=959),
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
