import pytest

from mex.common.models import (
    DistributionMapping,
    ExtractedContactPoint,
    ExtractedDistribution,
    ExtractedOrganization,
    ExtractedOrganizationalUnit,
    ExtractedPerson,
    ResourceMapping,
)
from mex.common.testing import Joker
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedResourceIdentifier,
    MergedVariableGroupIdentifier,
    TextLanguage,
)
from mex.extractors.open_data.models.source import (
    OpenDataCreatorsOrContributors,
    OpenDataParentResource,
    OpenDataTableSchema,
)
from mex.extractors.open_data.transform import (
    get_only_child_units,
    lookup_person_in_ldap_and_transform,
    transform_open_data_distributions,
    transform_open_data_parent_resource_to_mex_resource,
    transform_open_data_person_affiliations_to_organizations,
    transform_open_data_persons,
    transform_open_data_persons_not_in_ldap,
    transform_open_data_variable_groups,
    transform_open_data_variables,
)
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)


def test_get_only_child_units(
    mocked_extracted_organizational_units: list[ExtractedOrganizationalUnit],
) -> None:
    no_parent_units = get_only_child_units(
        [unit.stableTargetId for unit in mocked_extracted_organizational_units],
        mocked_extracted_organizational_units,
    )

    assert no_parent_units == [  # Parent unit PRNT is filtered out
        MergedOrganizationalUnitIdentifier("6rqNvZSApUHlz8GkkVP48"),  # C1
        MergedOrganizationalUnitIdentifier("cjna2jitPngp6yIV63cdi9"),  # FG 99
    ]


def test_transform_open_data_person_affiliations_to_organizations(
    mocked_open_data_creator_with_processed_affiliation: OpenDataCreatorsOrContributors,
) -> None:
    results = transform_open_data_person_affiliations_to_organizations(
        [mocked_open_data_creator_with_processed_affiliation],
    )
    assert results == {"Universität": Joker()}


def test_transform_open_data_persons_not_in_ldap_and_process_affiliation(
    mocked_open_data_creator_with_processed_affiliation: OpenDataCreatorsOrContributors,
    extracted_organization_rki: ExtractedOrganization,
) -> None:
    open_data_organization_ids_by_str = {
        "Universität": MergedOrganizationIdentifier.generate(seed=354)
    }

    results = transform_open_data_persons_not_in_ldap(
        mocked_open_data_creator_with_processed_affiliation,
        extracted_organization_rki,
        open_data_organization_ids_by_str,
    )
    assert results == ExtractedPerson(
        hadPrimarySource=get_extracted_primary_source_id_by_name("open-data"),
        identifierInPrimarySource="Resolved, Roland",
        fullName="Resolved, Roland",
        affiliation=[MergedOrganizationIdentifier.generate(seed=354)],
        orcidId=[],
    )


def test_transform_open_data_persons_not_in_ldap_and_ignore_affiliation(
    mocked_open_data_creator_with_affiliation_to_ignore: OpenDataCreatorsOrContributors,
    extracted_organization_rki: ExtractedOrganization,
) -> None:
    open_data_organization_ids_by_str = {
        "Universität": MergedOrganizationIdentifier.generate(seed=354),
        "RKI": extracted_organization_rki.stableTargetId,
    }
    results = transform_open_data_persons_not_in_ldap(
        mocked_open_data_creator_with_affiliation_to_ignore,
        extracted_organization_rki,
        open_data_organization_ids_by_str,
    )
    assert results == ExtractedPerson(
        hadPrimarySource=get_extracted_primary_source_id_by_name("open-data"),
        identifierInPrimarySource="Muster, Maxi",
        fullName="Muster, Maxi",
        affiliation=None,
        orcidId=[],
    )


@pytest.mark.usefixtures("mocked_ldap")
def test_lookup_person_in_ldap_and_transform(
    mocked_open_data_creator_with_affiliation_to_ignore: OpenDataCreatorsOrContributors,
    mocked_units_by_identifier_in_primary_source: dict[
        str, ExtractedOrganizationalUnit
    ],
    extracted_organization_rki: ExtractedOrganization,
) -> None:
    results = lookup_person_in_ldap_and_transform(
        mocked_open_data_creator_with_affiliation_to_ignore,
        mocked_units_by_identifier_in_primary_source,
        extracted_organization_rki,
    )
    assert results == ExtractedPerson(
        hadPrimarySource=get_extracted_primary_source_id_by_name("ldap"),
        identifierInPrimarySource="00000000-0000-4000-8000-000000000001",
        affiliation=[extracted_organization_rki.stableTargetId],
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
    mocked_extracted_organizational_units: list[ExtractedOrganizationalUnit],
    extracted_organization_rki: ExtractedOrganization,
) -> None:
    open_data_organization_ids_by_str = {
        "Universität": MergedOrganizationIdentifier.generate(seed=354)
    }
    results = transform_open_data_persons(
        [mocked_open_data_creator_with_affiliation_to_ignore],
        mocked_extracted_organizational_units,
        extracted_organization_rki,
        open_data_organization_ids_by_str,
    )

    assert results == [
        ExtractedPerson(
            hadPrimarySource=get_extracted_primary_source_id_by_name("ldap"),
            identifierInPrimarySource="00000000-0000-4000-8000-000000000001",
            affiliation=[extracted_organization_rki.stableTargetId],
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
    mocked_open_data_distribution_mapping: DistributionMapping,
) -> None:
    mex_distribution = transform_open_data_distributions(
        mocked_open_data_parent_resource,
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
    mocked_open_data_persons: list[ExtractedPerson],
    mocked_open_data_parent_resource_mapping: ResourceMapping,
    extracted_organization_rki: ExtractedOrganization,
    mocked_extracted_organizational_units: list[ExtractedOrganizationalUnit],
    mocked_open_data_extracted_contact_points: list[ExtractedContactPoint],
    mocked_open_data_distribution: list[ExtractedDistribution],
) -> None:
    unit_stable_target_ids_by_synonym = {
        unit.shortName[0].value: [unit.stableTargetId]
        for unit in mocked_extracted_organizational_units
    }

    mex_sources = list(
        transform_open_data_parent_resource_to_mex_resource(
            mocked_open_data_parent_resource,
            mocked_open_data_persons,
            unit_stable_target_ids_by_synonym,
            mocked_extracted_organizational_units,
            mocked_open_data_distribution,
            mocked_open_data_parent_resource_mapping,
            extracted_organization_rki,
            mocked_open_data_extracted_contact_points,
        )
    )

    assert len(mex_sources) == 1
    assert mex_sources[0].model_dump(exclude_none=True, exclude_defaults=True) == {
        "hadPrimarySource": str(get_extracted_primary_source_id_by_name("open-data")),
        "identifierInPrimarySource": "Eins",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-1",
        "created": "2021",
        "hasPersonalData": "https://mex.rki.de/item/personal-data-2",
        "license": "https://mex.rki.de/item/license-1",
        "contact": [str(mocked_open_data_extracted_contact_points[0].stableTargetId)],
        "theme": ["https://mex.rki.de/item/theme-1"],
        "title": [{"value": "Dumdidumdidum"}],
        "unitInCharge": [
            "6rqNvZSApUHlz8GkkVP48"
        ],  # only child unit of Maxi Muster = C1
        "anonymizationPseudonymization": [
            "https://mex.rki.de/item/anonymization-pseudonymization-1"
        ],
        "contributor": [str(mocked_open_data_persons[0].stableTargetId)],
        "contributingUnit": ["cjna2jitPngp6yIV63cdi9"],  # default, always unit "FG 99"
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


def test_transform_open_data_variable_groups(
    mocked_open_data_tableschemas_by_resource_id: dict[
        MergedResourceIdentifier, dict[str, list[OpenDataTableSchema]]
    ],
) -> None:
    result = transform_open_data_variable_groups(
        mocked_open_data_tableschemas_by_resource_id
    )

    assert result[0].model_dump(exclude_none=True, exclude_defaults=True) == {
        "hadPrimarySource": Joker(),
        "identifierInPrimarySource": "tableschema_lorem.json",
        "containedBy": ["LoremIpsumResourceId"],
        "label": [{"value": "tableschema_lorem"}],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }
    assert result[1].model_dump(exclude_none=True, exclude_defaults=True) == {
        "hadPrimarySource": Joker(),
        "identifierInPrimarySource": "tableschema_ipsum.json",
        "containedBy": ["LoremIpsumResourceId"],
        "label": [{"value": "tableschema_ipsum", "language": TextLanguage.EN}],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }
    assert result[2].model_dump(exclude_none=True, exclude_defaults=True) == {
        "hadPrimarySource": Joker(),
        "identifierInPrimarySource": "tableschema_dolor.json",
        "containedBy": ["DolorResourceId"],
        "label": [{"value": "tableschema_dolor"}],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }


def test_transform_open_data_variables(
    mocked_open_data_tableschemas_by_resource_id: dict[
        MergedResourceIdentifier, dict[str, list[OpenDataTableSchema]]
    ],
) -> None:
    mocked_merged_variable_group_id_by_filename = {
        "tableschema_lorem.json": MergedVariableGroupIdentifier("LoremVarGroupId"),
        "tableschema_ipsum.json": MergedVariableGroupIdentifier("IpsumVarGroupId"),
        "tableschema_dolor.json": MergedVariableGroupIdentifier("DolorVarGroupId"),
    }

    result = transform_open_data_variables(
        mocked_open_data_tableschemas_by_resource_id,
        mocked_merged_variable_group_id_by_filename,
    )

    assert result[0].model_dump(exclude_none=True, exclude_defaults=True) == {
        "hadPrimarySource": Joker(),
        "identifierInPrimarySource": "Lorem1_tableschema_lorem.json",
        "dataType": "string",
        "label": [{"value": "Lorem1"}],
        "usedIn": ["LoremIpsumResourceId"],
        "belongsTo": ["LoremVarGroupId"],
        "description": [{"value": "lorem 1"}],
        "valueSet": ["a, the letter 'a'", "b, and also 'b'"],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }
    assert result[1].model_dump(exclude_none=True, exclude_defaults=True) == {
        "hadPrimarySource": Joker(),
        "identifierInPrimarySource": "Lorem2_tableschema_lorem.json",
        "dataType": "string",
        "label": [{"value": "Lorem2"}],
        "usedIn": ["LoremIpsumResourceId"],
        "belongsTo": ["LoremVarGroupId"],
        "description": [{"value": "lorem 2"}],
        "valueSet": ["c", "d", "e", "f", "g"],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }
    assert result[2].model_dump(exclude_none=True, exclude_defaults=True) == {
        "hadPrimarySource": Joker(),
        "identifierInPrimarySource": "Ipsum_tableschema_ipsum.json",
        "dataType": "integer",
        "label": [{"value": "Ipsum"}],
        "usedIn": ["LoremIpsumResourceId"],
        "belongsTo": ["IpsumVarGroupId"],
        "description": [
            {"value": "no constraints and no categories", "language": Joker()}
        ],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }
    assert result[3].model_dump(exclude_none=True, exclude_defaults=True) == {
        "hadPrimarySource": Joker(),
        "identifierInPrimarySource": "Dolor_tableschema_dolor.json",
        "dataType": "boolean",
        "label": [{"value": "Dolor"}],
        "usedIn": ["DolorResourceId"],
        "belongsTo": ["DolorVarGroupId"],
        "description": [{"value": "dolor sit amet"}],
        "identifier": Joker(),
        "stableTargetId": Joker(),
    }
