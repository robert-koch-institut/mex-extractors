from typing import TYPE_CHECKING

import pytest

from mex.extractors.grippeweb.extract import (
    extract_columns_by_table_and_column_name,
    extract_grippeweb_ldap_person_ids_by_query,
    extract_grippeweb_organizations,
    extract_ldap_actors_for_functional_accounts,
)

if TYPE_CHECKING:
    from mex.common.models import (
        AccessPlatformMapping,
        ExtractedContactPoint,
        ExtractedOrganization,
        ExtractedPerson,
        ResourceMapping,
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
    contact_point: ExtractedContactPoint,
) -> None:
    ldap_actors = extract_ldap_actors_for_functional_accounts(
        grippeweb_resource_mappings
    )

    assert ldap_actors == {
        "contactc@rki.de": contact_point.stableTargetId,
    }


@pytest.mark.usefixtures("mocked_ldap", "mocked_grippeweb", "mocked_wikidata")
def test_extract_ldap_persons(
    grippeweb_resource_mappings: list[ResourceMapping],
    grippeweb_access_platform: AccessPlatformMapping,
    roland_resolved: ExtractedPerson,
) -> None:
    ldap_persons = extract_grippeweb_ldap_person_ids_by_query(
        grippeweb_resource_mappings, grippeweb_access_platform
    )

    assert ldap_persons == {
        "Roland Resolved": roland_resolved.stableTargetId,
        "resolvedr@rki.de": roland_resolved.stableTargetId,
    }


@pytest.mark.usefixtures("mocked_wikidata", "mocked_grippeweb")
def test_extract_grippeweb_organizations(
    grippeweb_resource_mappings: list[ResourceMapping],
    wikidata_extracted_organization: ExtractedOrganization,
) -> None:
    organizations = extract_grippeweb_organizations(grippeweb_resource_mappings)
    expected = {
        "Robert Koch-Institut": wikidata_extracted_organization.stableTargetId,
    }
    assert organizations == expected
