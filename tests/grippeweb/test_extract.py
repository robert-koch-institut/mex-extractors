from uuid import UUID

import pytest

from mex.common.ldap.models import LDAPActor, LDAPPerson
from mex.common.models import AccessPlatformMapping, ResourceMapping
from mex.common.types import MergedOrganizationIdentifier
from mex.extractors.grippeweb.extract import (
    extract_columns_by_table_and_column_name,
    extract_grippeweb_organizations,
    extract_ldap_actors_for_functional_accounts,
    extract_ldap_persons,
)


@pytest.mark.usefixtures("mocked_grippeweb")
def test_extract_columns_by_table_and_column_name() -> None:
    columns = extract_columns_by_table_and_column_name()
    expected = {
        "vActualQuestion": {
            "Id": ["AAA", "BBB"],
            "StartedOn": ["2023-11-01 00:00:00.0000000", "2023-12-01 00:00:00.0000000"],
            "FinishedOn": [
                "2023-12-01 00:00:00.0000000",
                "2024-01-01 00:00:00.0000000",
            ],
            "RepeatAfterDays": ["1", "2"],
        },
        "vWeeklyResponsesMEx": {
            "GuidTeilnehmer": [None, None],
            "Haushalt_Registrierer": [None, None],
        },
        "vMasterDataMEx": {
            "GuidTeilnehmer": [None, None],
            "Haushalt_Registrierer": [None, None],
        },
    }
    assert columns == expected


@pytest.mark.usefixtures("mocked_ldap", "mocked_grippeweb")
def test_extract_ldap_actors_for_functional_accounts(
    grippeweb_resource_mappings: list[ResourceMapping],
) -> None:
    ldap_actors = extract_ldap_actors_for_functional_accounts(
        grippeweb_resource_mappings
    )
    expected = LDAPActor(
        sAMAccountName="ContactC",
        objectGUID=UUID(int=4, version=4),
        mail=["email@email.de", "contactc@rki.de"],
    )

    assert ldap_actors[0] == expected


@pytest.mark.usefixtures("mocked_ldap", "mocked_grippeweb")
def test_extract_ldap_persons(
    grippeweb_resource_mappings: list[ResourceMapping],
    grippeweb_access_platform: AccessPlatformMapping,
) -> None:
    ldap_persons = extract_ldap_persons(
        grippeweb_resource_mappings, grippeweb_access_platform
    )
    expected = LDAPPerson(
        objectGUID=UUID(int=1, version=4),
        mail=["test_person@email.de"],
        department="PARENT-UNIT",
        displayName="Resolved, Roland",
        employeeID="42",
        givenName=["Roland"],
        sn="Resolved",
    )
    assert ldap_persons[0] == expected


@pytest.mark.usefixtures("mocked_wikidata", "mocked_grippeweb")
def test_extract_grippeweb_organizations(
    grippeweb_resource_mappings: list[ResourceMapping],
) -> None:
    organizations = extract_grippeweb_organizations(grippeweb_resource_mappings)
    expected = {
        "Robert Koch-Institut": MergedOrganizationIdentifier("ga6xh6pgMwgq7DC7r6Wjqg"),
    }
    assert organizations == expected
