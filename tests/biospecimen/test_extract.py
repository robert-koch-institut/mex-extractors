from typing import TYPE_CHECKING

import pandas as pd
import pytest
from pandas import Series

from mex.extractors.biospecimen.extract import (
    extract_biospecimen_contacts_by_email,
    extract_biospecimen_resources,
    get_clean_file_name,
    get_clean_string,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

    from mex.common.ldap.models import LDAPPerson
    from mex.extractors.biospecimen.models.source import BiospecimenResource


@pytest.mark.usefixtures("mocked_ldap")
def test_extract_biospecimen_contacts_by_email(
    biospecimen_resources: Iterable[BiospecimenResource],
    ldap_roland_resolved: LDAPPerson,
) -> None:
    ldap_persons = extract_biospecimen_contacts_by_email(biospecimen_resources)

    assert ldap_persons == [ldap_roland_resolved]


def test_extract_biospecimen_resources() -> None:
    resources = extract_biospecimen_resources()
    assert len(resources) == 2

    assert resources[0].model_dump(exclude_none=True) == {
        "file_name": "test_bioproben.xlsx",
        "offizieller_titel_der_probensammlung": ["test_titel"],
        "beschreibung": ["Testbeschreibung"],
        "schlagworte": ["Testschlagwort 1, Testschlagwort 2"],
        "methoden": ["Testmethode"],
        "zeitlicher_bezug": ["2021-09 bis 2021-10"],
        "rechte": "Testrechte",
        "studienbezug": ["1234567"],
        "alternativer_titel": "alternativer Testitel",
        "anonymisiert_pseudonymisiert": "pseudonymisiert",
        "externe_partner": "esterner Testpartner",
        "id_loinc": ["12345-6"],
        "id_mesh_begriff": ["D123"],
        "kontakt": ["resolvedr@rki.de"],
        "methodenbeschreibung": ["Testmethodenbeschreibung"],
        "mitwirkende_fachabteilung": "mitwirkende Testabteilung",
        "mitwirkende_personen": "mitwirkende Testperson",
        "raeumlicher_bezug": ["räumlicher Testbezug"],
        "ressourcentyp_allgemein": "Bioproben",
        "ressourcentyp_speziell": ["Infektionskrankheiten"],
        "sheet_name": "Probe1",
        "thema": [],
        "tools_instrumente_oder_apparate": "Testtool",
        "verantwortliche_fachabteilung": "PARENT Dept.",
        "verwandte_publikation_doi": "testverwandedoi",
        "verwandte_publikation_titel": "testverwandtepublikation",
        "vorhandene_anzahl_der_proben": "Testanzahl",
        "weiterfuehrende_dokumentation_titel": "Testdokutitel",
        "weiterfuehrende_dokumentation_url_oder_dateipfad": "Testdokupfad",
        "zugriffsbeschraenkung": "restriktiv",
    }


@pytest.mark.parametrize(
    ("file_name", "expected_clean_file_name"),
    [
        ("bioproben.xlsx", "bioprobenxlsx"),
        ("test_data", "testdata"),
        ("bioproben-data", "bioprobendata"),
    ],
)
def test_get_clean_file_name(file_name: str, expected_clean_file_name: str) -> None:
    clean_name = get_clean_file_name(file_name)

    assert clean_name == expected_clean_file_name


@pytest.mark.parametrize(
    ("series", "expected_clean_string"),
    [
        (
            pd.Series("Schlagwort b\n weiteres Schlagwort"),
            "Schlagwort b, weiteres Schlagwort",
        ),
        (
            pd.Series("Schlagwort b,   weiteres Schlagwort"),
            "Schlagwort b, weiteres Schlagwort",
        ),
        (
            pd.Series("Schlagwort b, weiteres Schlagwort,"),
            "Schlagwort b, weiteres Schlagwort",
        ),
    ],
)
def test_get_clean_string(series: Series, expected_clean_string: str) -> None:
    clean_string = get_clean_string(series)

    assert clean_string == expected_clean_string
