import pytest

from mex.common.ldap.models import LDAPPerson
from mex.common.models import ResourceMapping
from mex.common.types import MergedOrganizationIdentifier
from mex.extractors.voxco.extract import (
    extract_ldap_persons_voxco,
    extract_voxco_organizations,
    extract_voxco_variables,
)


@pytest.mark.usefixtures("mocked_drop")
def test_extract_voxco_variables() -> None:
    sources = extract_voxco_variables()
    expected = {
        "Id": 50614,
        "DataType": "Text",
        "Type": "Discrete",
        "QuestionText": "Monat",
        "Choices": [
            "@{Code=1; Text=Januar; Image=; HasOpenEnd=False; Visible=True; Default=False}",
            "@{Code=1; Text=Februar; Image=; HasOpenEnd=False; Visible=True; Default=False}",
        ],
        "Text": "Tag",
    }
    assert sources["voxco_data"][0].model_dump() == expected


@pytest.mark.usefixtures("mocked_wikidata")
def test_extract_voxco_organizations(
    voxco_resource_mappings: list[ResourceMapping],
) -> None:
    organizations = extract_voxco_organizations(voxco_resource_mappings)
    assert organizations == {
        "Robert Koch-Institut": MergedOrganizationIdentifier("ga6xh6pgMwgq7DC7r6Wjqg"),
    }


@pytest.mark.usefixtures("mocked_ldap")
def test_extract_ldap_persons_voxco(
    voxco_resource_mappings: list[ResourceMapping],
    ldap_roland_resolved: LDAPPerson,
) -> None:
    persons = extract_ldap_persons_voxco(voxco_resource_mappings)

    assert persons == [ldap_roland_resolved]
