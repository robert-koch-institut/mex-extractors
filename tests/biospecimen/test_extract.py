from typing import TYPE_CHECKING

import pandas as pd
import pytest
from pandas import Series

from mex.common.types import MergedPersonIdentifier
from mex.extractors.biospecimen.extract import (
    extract_biospecimen_contacts_by_email,
    extract_biospecimen_resources,
    get_clean_file_name,
    get_clean_string,
    get_year_from_zeitlicher_bezug,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

    from mex.extractors.biospecimen.models.source import BiospecimenResource
    from mex.extractors.settings import Settings


@pytest.mark.usefixtures("mocked_ldap")
def test_extract_biospecimen_contacts_by_email(
    biospecimen_resources: Iterable[BiospecimenResource],
) -> None:
    ldap_persons = extract_biospecimen_contacts_by_email(biospecimen_resources)

    assert ldap_persons == {
        "resolvedr@rki.de": MergedPersonIdentifier("eXA2Qj5pKmI7HXIgcVqCfz")
    }


def test_extract_biospecimen_resources() -> None:
    resources = extract_biospecimen_resources()
    assert len(resources) == 2

    assert resources[0].model_dump(exclude_none=True) == {
        "file_name": "test_bioproben.xlsx",
        "sheet_name": "Probe1",
        "zugriffsbeschraenkung": "restriktiv",
        "alternativer_titel": "alternativer Testitel",
        "anonymisiert_pseudonymisiert": "pseudonymisiert",
        "kontakt": ["resolvedr@rki.de"],
        "mitwirkende_fachabteilung": "mitwirkende Testabteilung",
        "mitwirkende_personen": "mitwirkende Testperson",
        "beschreibung": ["Testbeschreibung"],
        "weiterfuehrende_dokumentation_titel": "Testdokutitel",
        "weiterfuehrende_dokumentation_url_oder_dateipfad": "Testdokupfad",
        "externe_partner": "esterner Testpartner",
        "tools_instrumente_oder_apparate": "Testtool",
        "schlagworte": ["Testschlagwort 1, Testschlagwort 2"],
        "id_loinc": ["12345-6"],
        "id_mesh_begriff": ["D123"],
        "methoden": ["Testmethode"],
        "methodenbeschreibung": ["Testmethodenbeschreibung"],
        "verwandte_publikation_titel": "testverwandtepublikation",
        "verwandte_publikation_doi": "testverwandedoi",
        "ressourcentyp_allgemein": "Bioproben",
        "ressourcentyp_speziell": ["Infektionskrankheiten"],
        "rechte": "Testrechte",
        "vorhandene_anzahl_der_proben": "Testanzahl",
        "raeumlicher_bezug": ["räumlicher Testbezug"],
        "zeitlicher_bezug": ["2021-09 bis 2021-10"],
        "thema": [],
        "offizieller_titel_der_probensammlung": ["test_titel"],
        "verantwortliche_fachabteilung": "C1",
        "studienbezug": ["1234567"],
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


def test_get_year_from_zeitlicher_bezug(settings: Settings) -> None:
    key_col = settings.biospecimen.key_col
    val_col = settings.biospecimen.val_col

    test_df = None
    zeitlicher_bezug = get_year_from_zeitlicher_bezug(
        test_df, key_col, val_col, "zeitlicher Bezug"
    )
    assert zeitlicher_bezug is None

    test_df = pd.DataFrame(
        {key_col: ["zeitlicher Bezug"], val_col: ["Sept 2010-Dez 2012"]}
    )
    zeitlicher_bezug = get_year_from_zeitlicher_bezug(
        test_df, key_col, val_col, "zeitlicher Bezug"
    )
    assert zeitlicher_bezug == "2010"
