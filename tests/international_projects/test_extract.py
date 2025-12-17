import uuid

import pytest

from mex.common.types import TemporalEntity
from mex.extractors.international_projects.extract import (
    extract_international_projects_project_leaders,
    extract_international_projects_sources,
)


def test_extract_international_projects_sources() -> None:
    expected = [
        {
            "funding_type": "Third party funded",
            "project_lead_person": "Dr Frieda Ficticious",
            "end_date": TemporalEntity("2021-12-30"),
            "partner_organization": "",
            "funding_source": "Test-Institute",
            "funding_program": "",
            "rki_internal_project_number": "0000-1000",
            "additional_rki_units": "FG99",
            "project_lead_rki_unit": "FG99",
            "project_abbreviation": "testAAbr",
            "start_date": TemporalEntity("2021-07-26"),
            "activity1": "",
            "activity2": "",
            "topic1": "Laboratory diagnostics",
            "topic2": "",
            "full_project_name": "This is a test project full title",
            "website": "",
        },
        {
            "funding_type": "Third party funded",
            "project_lead_person": "Dr Frieda Ficticious",
            "end_date": TemporalEntity("2025-12-30"),
            "partner_organization": "",
            "funding_source": "Test-Institute",
            "funding_program": "GHPP2",
            "rki_internal_project_number": "0000-1000",
            "additional_rki_units": "",
            "project_lead_rki_unit": "FG18",
            "project_abbreviation": "testAAbr2",
            "start_date": TemporalEntity("2022-12-31"),
            "activity1": "Capacity building including trainings",
            "activity2": "Other",
            "topic1": "Laboratory diagnostics",
            "topic2": "Public health systems",
            "full_project_name": "This is a test project full title 2",
            "website": "does not exist yet",
        },
    ]

    international_projects_sources = extract_international_projects_sources()

    source_dicts = [
        s.model_dump(exclude_none=True) for s in international_projects_sources
    ]

    assert list(source_dicts[0]) == list(expected[0])
    assert list(source_dicts[1]) == list(expected[1])
    assert (
        source_dicts[0]["project_abbreviation"] == expected[0]["project_abbreviation"]
    )
    assert source_dicts[0]["funding_source"] == expected[0]["funding_source"]
    assert source_dicts[1]["full_project_name"] == expected[1]["full_project_name"]
    assert (
        source_dicts[1]["project_lead_rki_unit"] == expected[1]["project_lead_rki_unit"]
    )
    assert source_dicts[1]["funding_program"] == expected[1]["funding_program"]


@pytest.mark.usefixtures("mocked_ldap")
def test_extract_international_projects_project_leaders() -> None:
    expected = {
        "person": {
            "sAMAccountName": "ResolvedR",
            "objectGUID": uuid.UUID("00000000-0000-4000-8000-000000000001"),
            "mail": ["test_person@email.de"],
            "company": None,
            "department": "PARENT-UNIT",
            "departmentNumber": None,
            "displayName": "Resolved, Roland",
            "employeeID": "42",
            "givenName": ["Roland"],
            "ou": [],
            "sn": "Resolved",
        },
        "query": "Dr Frieda Ficticious",
    }
    international_projects_sources = extract_international_projects_sources()
    leaders = list(
        extract_international_projects_project_leaders(international_projects_sources)
    )

    assert leaders[0].model_dump() == expected
