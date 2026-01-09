import pytest
from pytest import MonkeyPatch

from mex.common.ldap.connector import LDAPConnector
from mex.extractors.blueant.extract import (
    extract_blueant_project_leaders,
    extract_blueant_sources,
    remove_prefixes_from_name,
)
from mex.extractors.blueant.models.source import BlueAntSource
from tests.blueant.mocked_blueant import MOCKED_API_SOURCE, MOCKED_RESOLVED_ATTRIBUTES


@pytest.mark.integration
@pytest.mark.requires_rki_infrastructure
def test_extract_blueant_sources_from_api() -> None:
    sources = extract_blueant_sources()
    assert sources[0].model_fields_set == BlueAntSource.model_fields.keys()


@pytest.mark.usefixtures("mocked_ldap", "mocked_blueant")
def test_extract_blueant_sources_from_api_mocked() -> None:
    expected_source = {
        k: v
        for k, v in MOCKED_API_SOURCE.items()
        if k not in ("clients", "departmentId", "projectLeaderId", "statusId", "typeId")
    } | MOCKED_RESOLVED_ATTRIBUTES

    sources = extract_blueant_sources()

    assert len(sources) == 1
    assert sources[0].model_dump() == expected_source


@pytest.mark.usefixtures("mocked_ldap")
def test_extract_blueant_project_leaders(blueant_source: BlueAntSource) -> None:
    blueant_sources = [blueant_source, blueant_source, blueant_source]
    persons = extract_blueant_project_leaders(blueant_sources)

    assert len(persons) == 1
    assert persons[0].employeeID == "42"


@pytest.mark.usefixtures("mocked_ldap")
def test_extract_blueant_project_leaders_not_in_ldap(
    monkeypatch: MonkeyPatch, blueant_source: BlueAntSource
) -> None:
    monkeypatch.setattr(
        LDAPConnector,
        "get_persons",
        lambda _, **__: [],
    )
    persons = extract_blueant_project_leaders([blueant_source])
    assert not persons


@pytest.mark.usefixtures("mocked_ldap")
def test_extract_blueant_project_leaders_from_source_without_leader(
    blueant_source: BlueAntSource,
) -> None:
    blueant_source.projectLeaderEmployeeId = None
    persons = extract_blueant_project_leaders([blueant_source])
    assert not persons


def test_remove_prefixes_from_name() -> None:
    assert (
        remove_prefixes_from_name("_Prototype Space Rocket") == "Prototype Space Rocket"
    )
