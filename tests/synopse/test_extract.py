from typing import TYPE_CHECKING

import pytest

from mex.common.types import MergedContactPointIdentifier, MergedPersonIdentifier
from mex.extractors.synopse.extract import (
    extract_projects,
    extract_study_data,
    extract_study_overviews,
    extract_synopse_contact,
    extract_synopse_project_contributor_ids_by_query,
    extract_variables,
)

if TYPE_CHECKING:
    from mex.common.models import AccessPlatformMapping
    from mex.extractors.synopse.models.project import ProjektUndStudienverwaltung


def test_extract_variables() -> None:
    expected_first_variable = {
        "textbox5": "Krankheiten (1101)",
        "val_instrument": None,
        "textbox11": "Zahl",
        "originalfrage": None,
        "symopse_id": "503",
        "textbox21": "Lorem Symptome: Halsschmerzen (1. Tag)",
        "textbox24": "KLMNO_F4",
        "textbox51": "Eingangsfrage wn/kA",
        "int_var": False,
        "studie_id1": "BBCCDD1",
        "studie_id2": 1122999,
    }
    expected_second_variable = {
        "textbox5": "Krankheiten (1101)",
        "val_instrument": None,
        "textbox11": "Zahl",
        "originalfrage": None,
        "symopse_id": "503",
        "textbox21": "Lorem Symptome: Halsschmerzen (1. Tag)",
        "textbox24": "KLMNO_F4",
        "textbox51": "Weiß nicht",
        "int_var": False,
        "studie_id1": "BBCCDD1",
        "studie_id2": 1122999,
    }
    variables = extract_variables()
    assert len(variables) == 16
    assert variables[0].model_dump() == expected_first_variable
    assert variables[4].model_dump() == expected_second_variable


def test_extract_study_data() -> None:
    expected_study_data = {
        "studien_id": "1234567",
        "dstyp_id": 17,
        "titel_datenset": "BBCCDD",
        "beschreibung": "BBCCDD Basiserhebung, Kohorte",
        "schlagworte_themen": "BBCCDD Basiserhebung, Kohorte",
        "rechte": "restriktiv",
        "zugangsbeschraenkung": "S:BBCCDD-Basis Variablennamen - XYZ-Reports",
    }
    study_data = extract_study_data()
    assert len(study_data) == 2
    assert study_data[0].model_dump(exclude_defaults=True) == expected_study_data


def test_extract_projects() -> None:
    expected_project = {
        "studien_id": "1122999",
        "studie": "BBCCDD1",
        "studien_art_typ": "Monitoring-Studie",
        "projekt_studientitel": "Mit der BBCCDD-Basiserhebung hat das Robert Koch-Institut umfassende Daten zur Gesundheit der in Deutschland lebenden Lorems gesammelt. Das Studienprogramm umfasste neben Befragungen auch Ipsumalysen.",
        "beschreibung_studie": "fg@example.com",
    }
    projects = extract_projects()
    assert len(projects) == 4
    assert projects[0].model_dump(exclude_none=True) == expected_project


@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_extract_synopse_project_contributor_ids_by_query(
    synopse_project: ProjektUndStudienverwaltung,
) -> None:
    persons = extract_synopse_project_contributor_ids_by_query(
        [synopse_project, synopse_project]
    )
    assert persons == {
        "Roland Resolved": [MergedPersonIdentifier("eXA2Qj5pKmI7HXIgcVqCfz")]
    }


@pytest.mark.usefixtures("mocked_ldap")
def test_extract_synopse_contact(
    synopse_access_platform: AccessPlatformMapping,
) -> None:
    actor = extract_synopse_contact(synopse_access_platform)

    assert actor == {
        "contactc@rki.de": MergedContactPointIdentifier("cMkmnNOoNVAohBA1XLNr9K"),
    }


def test_extract_study_overviews() -> None:
    expected_overview = {
        "dstyp_id": 15,
        "studien_id": "1234567",
        "synopse_id": "405",
        "titel_datenset": "BBCCDD",
    }
    overviews = extract_study_overviews()
    assert len(overviews) == 9
    assert overviews[0].model_dump() == expected_overview
