from typing import TYPE_CHECKING

import pytest

from mex.common.exceptions import MExError
from mex.common.models import ExtractedOrganization
from mex.common.types import (
    MergedOrganizationIdentifier,
    MergedResourceIdentifier,
    MergedVariableGroupIdentifier,
    MIMEType,
    TextLanguage,
)
from mex.extractors.open_data.transform import (
    find_mime_type,
    get_only_child_units,
    get_or_transform_open_data_persons,
    transform_and_load_open_data_persons_not_in_ldap,
    transform_open_data_distributions,
    transform_open_data_parent_resource_to_mex_resource,
    transform_open_data_person_affiliations_to_organizations,
    transform_open_data_variable_groups,
    transform_open_data_variables,
)
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)

if TYPE_CHECKING:
    from mex.common.models import (
        DistributionMapping,
        ExtractedContactPoint,
        ExtractedDistribution,
        ExtractedOrganizationalUnit,
        ExtractedPerson,
        ResourceMapping,
    )
    from mex.extractors.open_data.models.source import (
        OpenDataCreatorsOrContributors,
        OpenDataParentResource,
        OpenDataTableSchema,
    )


def test_get_only_child_units(
    mocked_extracted_organizational_units: list[ExtractedOrganizationalUnit],
    mocked_units_by_identifier_in_primary_source: dict[
        str, ExtractedOrganizationalUnit
    ],
) -> None:
    no_parent_units = get_only_child_units(
        [unit.stableTargetId for unit in mocked_extracted_organizational_units],
        mocked_extracted_organizational_units,
    )

    assert no_parent_units == [  # Parent unit PRNT is filtered out
        mocked_units_by_identifier_in_primary_source["child-unit"].stableTargetId,  # C1
        mocked_units_by_identifier_in_primary_source["fg99"].stableTargetId,  # FG 99
    ]


def test_transform_open_data_person_affiliations_to_organizations(
    mocked_open_data_creator_with_processed_affiliation: OpenDataCreatorsOrContributors,
) -> None:
    results = transform_open_data_person_affiliations_to_organizations(
        [mocked_open_data_creator_with_processed_affiliation],
    )
    created_organization = ExtractedOrganization(
        officialName="Universität",
        identifierInPrimarySource="Universität",
        hadPrimarySource=get_extracted_primary_source_id_by_name("open-data"),
    )
    assert results == {"Universität": created_organization.stableTargetId}


def test_transform_and_load_open_data_persons_not_in_ldap_and_process_affiliation(
    mocked_open_data_creator_with_processed_affiliation: OpenDataCreatorsOrContributors,
    extracted_organization_rki: ExtractedOrganization,
) -> None:
    open_data_organization_ids_by_str = {
        "Universität": MergedOrganizationIdentifier.generate(seed=354)
    }

    person = transform_and_load_open_data_persons_not_in_ldap(
        mocked_open_data_creator_with_processed_affiliation,
        extracted_organization_rki,
        open_data_organization_ids_by_str,
    )
    assert person.model_dump(exclude_defaults=True) == {
        "hadPrimarySource": get_extracted_primary_source_id_by_name("open-data"),
        "identifierInPrimarySource": "Resolved, Roland",
        "affiliation": ["bFQoRhcVH5DHZ8"],
        "fullName": ["Resolved, Roland"],
        "identifier": "cOIOKJr1u78e6AxfUFAJ4u",
        "orcidId": ["https://orcid.org/9876543210"],
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
        extracted_organization_rki,
        open_data_organization_ids_by_str,
    )
    assert person.model_dump(exclude_defaults=True) == {
        "hadPrimarySource": get_extracted_primary_source_id_by_name("open-data"),
        "identifierInPrimarySource": "Felicitás, Juturna",
        "fullName": ["Felicitás, Juturna"],
        "identifier": "gfCLiDlCRkSTXic8WuawRm",
        "orcidId": ["https://orcid.org/0000-0002-1234-5678"],
        "stableTargetId": "hA4zlscf5H7HsfqZYMGqXT",
    }


@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_transform_open_data_persons(
    mocked_open_data_creator_with_affiliation_to_ignore: OpenDataCreatorsOrContributors,
    extracted_organization_rki: ExtractedOrganization,
    mocked_units_by_identifier_in_primary_source: dict[
        str, ExtractedOrganizationalUnit
    ],
    wikidata_extracted_organization: ExtractedOrganization,
    juturna_felicitas: ExtractedPerson,
) -> None:
    open_data_organization_ids_by_str = {
        "RKI": MergedOrganizationIdentifier.generate(seed=354)
    }
    persons = get_or_transform_open_data_persons(
        [mocked_open_data_creator_with_affiliation_to_ignore],
        extracted_organization_rki,
        open_data_organization_ids_by_str,
    )
    assert len(persons) == 1
    assert persons[0].model_dump(exclude_defaults=True) == {
        "affiliation": [wikidata_extracted_organization.stableTargetId],
        "email": ["felicitasj@rki.de"],
        "familyName": ["Felicitás"],
        "fullName": ["Felicitás, Juturna"],
        "givenName": ["Juturna"],
        "hadPrimarySource": get_extracted_primary_source_id_by_name("ldap"),
        "identifierInPrimarySource": "00000000-0000-4000-8000-000000000002",
        "identifier": "hiY0YTC5dUkBrf8ujMemjh",
        "memberOf": [
            mocked_units_by_identifier_in_primary_source["fg99"].stableTargetId
        ],
        "orcidId": ["https://orcid.org/0000-0002-1234-5678"],
        "stableTargetId": juturna_felicitas.stableTargetId,
    }


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (
            "text/csv",
            MIMEType("https://mex.rki.de/item/mime-type-7"),
        ),
        (
            "csv",
            MIMEType("https://mex.rki.de/item/mime-type-7"),
        ),
        (
            "tab-separated-values",
            MIMEType("https://mex.rki.de/item/mime-type-13"),
        ),
        (
            "text/tab-separated-values",
            MIMEType("https://mex.rki.de/item/mime-type-13"),
        ),
        (
            "application/json",
            MIMEType("https://mex.rki.de/item/mime-type-19"),
        ),
        (
            "https://www.iana.org/assignments/media-types/application/pdf",
            MIMEType("https://mex.rki.de/item/mime-type-4"),
        ),
        ("comma-separated-values", None),
        ("tab separated values", None),
        ("TSV", None),
        ("does not exist", None),
        ("", None),
        (None, None),
    ],
)
def test_find_mime_type(
    value: str | None,
    expected: MIMEType | None,
) -> None:
    assert find_mime_type(value) == expected


def test_find_mime_type_raises_for_multiple_matches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "mex.extractors.open_data.transform._get_mime_type_exact_matches",
        lambda: (
            (
                "https://www.iana.org/assignments/media-types/application/example",
                MIMEType("https://mex.rki.de/item/mime-type-1"),
            ),
            (
                "https://www.iana.org/assignments/media-types/application/example-other",
                MIMEType("https://mex.rki.de/item/mime-type-2"),
            ),
        ),
    )

    with pytest.raises(
        MExError,
        match=r"Found more than one match for mime-type search pattern 'application'",
    ):
        find_mime_type("application")


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
        "hadPrimarySource": get_extracted_primary_source_id_by_name("open-data"),
        "accessURL": [{"url": "https://doi.org/10.3456/zenodo.7890"}],
        "identifierInPrimarySource": "file_test_id",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-1",
        "issued": "2021-01-01T01:01:01Z",
        "license": "https://mex.rki.de/item/license-1",
        "title": [{"value": "some text"}],
        "mediaType": "https://mex.rki.de/item/mime-type-7",
        "downloadURL": [{"url": "www.efg.hi"}],
        "identifier": "dHGPzfgNnnEuzEWqAUQw5L",
        "stableTargetId": "hh1vwUYabPZCAlUislSGfu",
    }


@pytest.mark.usefixtures("mocked_ldap", "mocked_open_data", "mocked_wikidata")
def test_transform_open_data_parent_resource_to_mex_resource(  # noqa: PLR0913, PLR0917
    mocked_open_data_parent_resource: list[OpenDataParentResource],
    juturna_felicitas: ExtractedPerson,
    mocked_open_data_parent_resource_mapping: ResourceMapping,
    extracted_organization_rki: ExtractedOrganization,
    mocked_extracted_organizational_units: list[ExtractedOrganizationalUnit],
    mocked_open_data_distribution: list[ExtractedDistribution],
    contact_point: ExtractedContactPoint,
    mocked_units_by_identifier_in_primary_source: dict[
        str, ExtractedOrganizationalUnit
    ],
) -> None:
    mex_sources = transform_open_data_parent_resource_to_mex_resource(
        mocked_open_data_parent_resource,
        [juturna_felicitas],
        mocked_extracted_organizational_units,
        mocked_open_data_distribution,
        mocked_open_data_parent_resource_mapping,
        extracted_organization_rki,
        [contact_point],
    )

    assert len(mex_sources) == 1
    assert mex_sources[0].model_dump(exclude_none=True, exclude_defaults=True) == {
        "hadPrimarySource": get_extracted_primary_source_id_by_name("open-data"),
        "identifierInPrimarySource": "Eins",
        "accessRestriction": "https://mex.rki.de/item/access-restriction-1",
        "start": ["2021"],
        "hasPersonalData": "https://mex.rki.de/item/personal-data-2",
        "landingPage": [
            {"language": TextLanguage.DE, "url": "123", "title": "Datensatz"},
            {"language": TextLanguage.EN, "url": "123", "title": "Dataset"},
        ],
        "license": "https://mex.rki.de/item/license-1",
        "contact": [contact_point.stableTargetId],
        "theme": ["https://mex.rki.de/item/theme-1"],
        "title": [{"value": "Dumdidumdidum"}],
        "unitInCharge": [
            mocked_units_by_identifier_in_primary_source["fg99"].stableTargetId
        ],  # only child unit of Juturna Felicitás = C1
        "anonymizationPseudonymization": [
            "https://mex.rki.de/item/anonymization-pseudonymization-1"
        ],
        "contributor": [juturna_felicitas.stableTargetId],
        "contributingUnit": [
            mocked_units_by_identifier_in_primary_source["fg99"].stableTargetId
        ],  # default, always unit "FG 99"
        "description": [
            {"value": "Test1 <a href='test/2'>test3</a>", "language": TextLanguage.EN}
        ],
        "doi": "https://doi.org/10.3456/zenodo.7890",
        "distribution": [mocked_open_data_distribution[0].stableTargetId],
        "publisher": [extracted_organization_rki.stableTargetId],
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
        "hadPrimarySource": get_extracted_primary_source_id_by_name("open-data"),
        "identifierInPrimarySource": "tableschema_lorem.json",
        "containedBy": ["LoremIpsumResourceId"],
        "label": [{"value": "tableschema_lorem"}],
        "identifier": "ftl4bWJd6ACRjdnfRGcGii",
        "stableTargetId": "fIippLaxM3OB3PHuas7KE9",
    }
    assert result[1].model_dump(exclude_none=True, exclude_defaults=True) == {
        "hadPrimarySource": get_extracted_primary_source_id_by_name("open-data"),
        "identifierInPrimarySource": "tableschema_ipsum.json",
        "containedBy": ["LoremIpsumResourceId"],
        "label": [{"value": "tableschema_ipsum", "language": TextLanguage.EN}],
        "identifier": "hEH4MPVFJlcdCgCSwRvr0r",
        "stableTargetId": "cn1c9ewpNgLGuWlxwQ5Pn8",
    }
    assert result[2].model_dump(exclude_none=True, exclude_defaults=True) == {
        "hadPrimarySource": get_extracted_primary_source_id_by_name("open-data"),
        "identifierInPrimarySource": "tableschema_dolor.json",
        "containedBy": ["DolorResourceId"],
        "label": [{"value": "tableschema_dolor"}],
        "identifier": "cFYQLvfOaxZAoZVr71wHcN",
        "stableTargetId": "fXYBRtMb47Fg1qUP82TprS",
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
        "hadPrimarySource": get_extracted_primary_source_id_by_name("open-data"),
        "identifierInPrimarySource": "Lorem1_tableschema_lorem.json",
        "dataType": "string",
        "label": [{"value": "Lorem1"}],
        "usedIn": ["LoremIpsumResourceId"],
        "belongsTo": ["LoremVarGroupId"],
        "description": [{"value": "lorem 1"}],
        "valueSet": ["a, the letter 'a'", "b, and also 'b'"],
        "identifier": "fU1M8H1otofz9yOzNVmTWB",
        "stableTargetId": "ctTj4ERtPiMVcjspBSMO1y",
    }
    assert result[1].model_dump(exclude_none=True, exclude_defaults=True) == {
        "hadPrimarySource": get_extracted_primary_source_id_by_name("open-data"),
        "identifierInPrimarySource": "Lorem2_tableschema_lorem.json",
        "dataType": "string",
        "label": [{"value": "Lorem2"}],
        "usedIn": ["LoremIpsumResourceId"],
        "belongsTo": ["LoremVarGroupId"],
        "description": [{"value": "lorem 2"}],
        "valueSet": ["c", "d", "e", "f", "g"],
        "identifier": "bmDAgdk9oL2DkkKKk1bH0i",
        "stableTargetId": "blXjdPO52BX9hSmO54CxKu",
    }
    assert result[2].model_dump(exclude_none=True, exclude_defaults=True) == {
        "hadPrimarySource": get_extracted_primary_source_id_by_name("open-data"),
        "identifierInPrimarySource": "Ipsum_tableschema_ipsum.json",
        "dataType": "integer",
        "label": [{"value": "Ipsum"}],
        "usedIn": ["LoremIpsumResourceId"],
        "belongsTo": ["IpsumVarGroupId"],
        "description": [
            {"value": "no constraints and no categories", "language": TextLanguage.EN}
        ],
        "identifier": "ex1syL7NIvYIT8S9d7N6FR",
        "stableTargetId": "f7IDWo790NgOpeygCYLj8o",
    }
    assert result[3].model_dump(exclude_none=True, exclude_defaults=True) == {
        "hadPrimarySource": get_extracted_primary_source_id_by_name("open-data"),
        "identifierInPrimarySource": "Dolor_tableschema_dolor.json",
        "dataType": "boolean",
        "label": [{"value": "Dolor"}],
        "usedIn": ["DolorResourceId"],
        "belongsTo": ["DolorVarGroupId"],
        "description": [{"value": "dolor sit amet"}],
        "identifier": "fICHMnkiisF4suvb4gzJEw",
        "stableTargetId": "D2hKMPvjhNM8E4n13Osm3",
    }
