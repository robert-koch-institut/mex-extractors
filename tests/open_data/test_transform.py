from typing import TYPE_CHECKING

import pytest

from mex.common.testing import Joker
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedResourceIdentifier,
    MergedVariableGroupIdentifier,
    TextLanguage,
)
from mex.extractors.open_data.transform import (
    get_or_transform_open_data_persons,
    get_unit_ids_of_parent_units,
    transform_and_load_open_data_persons_not_in_ldap,
    transform_open_data_distributions,
    transform_open_data_parent_resource_to_mex_resource,
    transform_open_data_person_affiliations_to_organizations,
    transform_open_data_variable_groups,
    transform_open_data_variables,
)

if TYPE_CHECKING:
    from mex.common.models import (
        DistributionMapping,
        ExtractedContactPoint,
        ExtractedDistribution,
        ExtractedOrganization,
        ExtractedPerson,
        ResourceMapping,
    )
    from mex.extractors.open_data.models.source import (
        OpenDataCreatorsOrContributors,
        OpenDataParentResource,
        OpenDataTableSchema,
    )


def test_get_only_child_units() -> None:
    no_parent_units = get_unit_ids_of_parent_units()

    assert list(no_parent_units) == [
        MergedOrganizationalUnitIdentifier("hIiJpZXVppHvoyeP0QtAoS")
    ]


def test_transform_open_data_person_affiliations_to_organizations(
    mocked_open_data_creator_with_processed_affiliation: OpenDataCreatorsOrContributors,
) -> None:
    results = transform_open_data_person_affiliations_to_organizations(
        [mocked_open_data_creator_with_processed_affiliation],
    )
    assert results == {"Universität": Joker()}


def test_transform_and_load_open_data_persons_not_in_ldap_and_process_affiliation(
    mocked_open_data_creator_with_processed_affiliation: OpenDataCreatorsOrContributors,
) -> None:
    open_data_organization_ids_by_str = {
        "Universität": MergedOrganizationIdentifier.generate(seed=354)
    }

    person = transform_and_load_open_data_persons_not_in_ldap(
        mocked_open_data_creator_with_processed_affiliation,
        open_data_organization_ids_by_str,
    )
    assert person.model_dump(exclude_defaults=True) == {
        "hadPrimarySource": "bEwCy4xNTx9gCJr9aJ7LM",
        "identifierInPrimarySource": "Resolved, Roland",
        "affiliation": ["bFQoRhcVH5DHZ8"],
        "fullName": ["Resolved, Roland"],
        "orcidId": ["https://orcid.org/9876543210"],
        "identifier": "cOIOKJr1u78e6AxfUFAJ4u",
        "stableTargetId": "Y54iBGnQ5BCx4Hb7sMt7L",
    }


def test_transform_and_load_open_data_persons_not_in_ldap_and_ignore_affiliation(
    mocked_open_data_creator_with_affiliation_to_ignore: OpenDataCreatorsOrContributors,
    extracted_organization_rki: ExtractedOrganization,
) -> None:
    open_data_organization_ids_by_str = {
        "Universität": MergedOrganizationIdentifier.generate(seed=354),
        "RKI": extracted_organization_rki.stableTargetId,
    }
    person = transform_and_load_open_data_persons_not_in_ldap(
        mocked_open_data_creator_with_affiliation_to_ignore,
        open_data_organization_ids_by_str,
    )
    assert person.model_dump(exclude_defaults=True) == {
        "hadPrimarySource": "bEwCy4xNTx9gCJr9aJ7LM",
        "identifierInPrimarySource": "Felicitás, Juturna",
        "affiliation": ["fxIeF3TWocUZoMGmBftJ6x"],
        "fullName": ["Felicitás, Juturna"],
        "orcidId": ["https://orcid.org/0000-0002-1234-5678"],
        "identifier": "gfCLiDlCRkSTXic8WuawRm",
        "stableTargetId": "hA4zlscf5H7HsfqZYMGqXT",
    }


@pytest.mark.usefixtures("mocked_ldap")
def test_transform_open_data_persons(
    mocked_open_data_creator_with_affiliation_to_ignore: OpenDataCreatorsOrContributors,
) -> None:
    open_data_organization_ids_by_str = {
        "RKI": MergedOrganizationIdentifier.generate(seed=354)
    }
    persons = get_or_transform_open_data_persons(
        [mocked_open_data_creator_with_affiliation_to_ignore],
        open_data_organization_ids_by_str,
    )
    assert len(persons) == 1
    assert persons[0].model_dump(exclude_defaults=True) == {
        "hadPrimarySource": "ebs5siX85RkdrhBRlsYgRP",
        "identifierInPrimarySource": "00000000-0000-4000-8000-000000000002",
        "affiliation": ["ga6xh6pgMwgq7DC7r6Wjqg"],
        "email": ["felicitasj@rki.de"],
        "familyName": ["Felicitás"],
        "fullName": ["Felicitás, Juturna"],
        "givenName": ["Juturna"],
        "identifier": "hiY0YTC5dUkBrf8ujMemjh",
        "stableTargetId": "cpKNwpoZTQ4GpIzBgO8DMx",
    }


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


@pytest.mark.usefixtures("mocked_ldap", "mocked_open_data", "mocked_wikidata")
def test_transform_open_data_parent_resource_to_mex_resource(  # noqa: PLR0913
    mocked_open_data_parent_resource: list[OpenDataParentResource],
    juturna_felicitas: ExtractedPerson,
    mocked_open_data_parent_resource_mapping: ResourceMapping,
    extracted_organization_rki: ExtractedOrganization,
    mocked_open_data_distribution: list[ExtractedDistribution],
    contact_point: ExtractedContactPoint,
) -> None:
    mex_sources = transform_open_data_parent_resource_to_mex_resource(
        mocked_open_data_parent_resource,
        [juturna_felicitas],
        mocked_open_data_distribution,
        mocked_open_data_parent_resource_mapping,
        extracted_organization_rki,
        [contact_point],
    )

    assert len(mex_sources) == 1
    assert mex_sources[0].model_dump(exclude_none=True, exclude_defaults=True) == {
        "hadPrimarySource": "bEwCy4xNTx9gCJr9aJ7LM",
        "identifierInPrimarySource": "Eins",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-1",
        "created": "2021",
        "doi": "https://doi.org/10.3456/zenodo.7890",
        "hasPersonalData": "https://mex.rki.de/item/personal-data-2",
        "license": "https://mex.rki.de/item/license-1",
        "contact": ["cMkmnNOoNVAohBA1XLNr9K"],
        "theme": ["https://mex.rki.de/item/theme-1"],
        "title": [{"value": "Dumdidumdidum"}],
        "unitInCharge": ["bFQoRhcVH5IS5j"],
        "anonymizationPseudonymization": [
            "https://mex.rki.de/item/anonymization-pseudonymization-1"
        ],
        "contributingUnit": ["bFQoRhcVH5IS5j"],
        "contributor": ["cpKNwpoZTQ4GpIzBgO8DMx"],
        "description": [
            {"value": "Test1 <a href='test/2'>test3</a>", "language": "en"}
        ],
        "distribution": ["dMXXtqkXco7A1FslVtJWqR"],
        "publisher": ["fxIeF3TWocUZoMGmBftJ6x"],
        "resourceTypeGeneral": ["https://mex.rki.de/item/resource-type-general-14"],
        "identifier": "eKh2uKxQPKQ9Z9bamF2mxy",
        "stableTargetId": "cV3l5BLBycXDZDbWA5ipwl",
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
