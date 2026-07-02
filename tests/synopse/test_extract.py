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


def test_extract_variables() -> None:
    expected_first_variable = {
        "auspraegungen": "-95",
        "datentyp": "Zahl",
        "int_var": False,
        "keep_varname": True,
        "originalfrage": None,
        "studie": "BBCCDD1",
        "StudieID2": 1122999,
        "SynopseID": "503",
        "text_dt": "Eingangsfrage wn/kA",
        "textbox5": "Krankheiten (1101)",
        "unterthema": "Lorem (110116)",
        "val_instrument": None,
        "varlabel": "Lorem Symptome: Halsschmerzen (1. Tag)",
        "varname": "KLMNO_F4",
    }
    expected_second_variable = {
        "auspraegungen": "-98",
        "datentyp": "Zahl",
        "int_var": False,
        "keep_varname": True,
        "originalfrage": None,
        "studie": "BBCCDD1",
        "StudieID2": 1122999,
        "SynopseID": "503",
        "text_dt": "Weiß nicht",
        "textbox5": "Krankheiten (1101)",
        "unterthema": "Lorem (110116)",
        "val_instrument": None,
        "varlabel": "Lorem Symptome: Halsschmerzen (1. Tag)",
        "varname": "KLMNO_F4",
    }
    variables = extract_variables()
    assert len(variables) == 16
    assert variables[0].model_dump() == expected_first_variable
    assert variables[4].model_dump() == expected_second_variable


def test_extract_study_data() -> None:
    expected_study_data = {
        "Beschreibung": "BBCCDD Basiserhebung, Kohorte",
        "dateiformat": "sas,stata",
        "DStypID": 17,
        "erstellungs_datum": "2013",
        "lizenz": "Reportserver",
        "plattform": '"Z:\\Lorem\\Ipsum\\DATA\\BBCCDD\\Dokumentation',
        "rechte": "restriktiv",
        "schlagworte_themen": "BBCCDD Basiserhebung, Kohorte",
        "studie": "BBCCDD",
        "StudienID": "1234567",
        "Titel_Datenset": "BBCCDD",
        "version": "V26",
        "zugangsbeschraenkung": "S:BBCCDD-Basis Variablennamen - XYZ-Reports",
    }
    study_data = extract_study_data()
    assert len(study_data) == 2
    assert study_data[0].model_dump(exclude_defaults=True) == expected_study_data


def test_extract_projects() -> None:
    expected_project = {
        "Studie": "BBCCDD1",
        "ProjektStudientitel": "Mit der BBCCDD-Basiserhebung hat das Robert Koch-Institut umfassende Daten zur Gesundheit der in Deutschland lebenden Lorems gesammelt. Das Studienprogramm umfasste neben Befragungen auch Ipsumalysen.",
        "BeschreibungStudie": "fg@example.com",
        "StudienArtTyp": "Monitoring-Studie",
        "StudienID": "1122999",
        "verknuepfteRessourcen": "Studie zu Lorem und Ipsum",
        "SchlagworteStudie": "CHLD",
    }
    projects = extract_projects()
    assert len(projects) == 4
    assert projects[0].model_dump(exclude_none=True) == expected_project


@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_extract_synopse_project_contributor_ids_by_query(
    synopse_project: SynopseProject,
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
        "DStypID": 15,
        "StudienID": "1234567",
        "SynopseID": "405",
        "Titel_Datenset": "BBCCDD",
    }
    overviews = extract_study_overviews()
    assert len(overviews) == 9
    assert overviews[0].model_dump() == expected_overview
