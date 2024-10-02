from typing import Any
from uuid import UUID

import pytest

from mex.common.ldap.models.actor import LDAPActor
from mex.common.ldap.models.person import LDAPPerson
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
    grippeweb_resource_mappings: list[dict[str, Any]],
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
    grippeweb_resource_mappings: list[dict[str, Any]],
    grippeweb_access_platform: dict[str, Any],
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
    grippeweb_resource_mappings: list[dict[str, Any]],
) -> None:
    organizations = extract_grippeweb_organizations(grippeweb_resource_mappings)
    expected = {
        "identifier": "Q679041",
        "labels": {
            "de": {"language": "de", "value": "Robert Koch-Institut"},
            "en": {"language": "en", "value": "Robert Koch Institute"},
        },
        "claims": {
            "website": [
                {"mainsnak": {"datavalue": {"value": {"text": "https://www.rki.de/"}}}},
                {
                    "mainsnak": {
                        "datavalue": {
                            "value": {
                                "text": "https://www.rki.de/DE/Home/homepage_node.html"
                            }
                        }
                    }
                },
                {
                    "mainsnak": {
                        "datavalue": {
                            "value": {
                                "text": "https://www.rki.de/EN/Home/homepage_node.html"
                            }
                        }
                    }
                },
            ],
            "isni_id": [
                {"mainsnak": {"datavalue": {"value": {"text": "0000 0001 0940 3744"}}}}
            ],
            "ror_id": [{"mainsnak": {"datavalue": {"value": {"text": "01k5qnb77"}}}}],
            "short_name": [
                {
                    "mainsnak": {
                        "datavalue": {"value": {"text": "RKI", "language": "en"}}
                    }
                },
                {
                    "mainsnak": {
                        "datavalue": {"value": {"text": "RKI", "language": "de"}}
                    }
                },
                {
                    "mainsnak": {
                        "datavalue": {"value": {"text": "IRK", "language": "fr"}}
                    }
                },
            ],
        },
        "aliases": {
            "de": [
                {"language": "de", "value": "alias_de_1"},
                {"language": "de", "value": "alias_de_2"},
                {"language": "de", "value": "alias_de_3"},
            ],
            "en": [
                {"language": "en", "value": "alias_en_1"},
                {"language": "en", "value": "alias_en_2"},
                {"language": "en", "value": "alias_en_3"},
                {"language": "en", "value": "alias_en_4"},
            ],
        },
    }
    assert (
        organizations["Robert Koch-Institut"].model_dump(
            exclude_none=True, exclude_defaults=True
        )
        == expected
    )
