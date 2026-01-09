import pytest

from mex.common.models import ResourceMapping
from mex.extractors.biospecimen.models.source import BiospecimenResource
from mex.extractors.settings import Settings
from mex.extractors.utils import load_yaml


@pytest.fixture
def biospecimen_resources() -> list[BiospecimenResource]:
    """Return a dummy biospecimen resource for testing."""
    return [
        BiospecimenResource(
            file_name=["test_bioproben"],
            offizieller_titel_der_probensammlung=["test_titel"],
            beschreibung=["Testbeschreibung"],
            schlagworte=["Testschlagwort 1, Testschlagwort 2"],
            methoden=["Testmethode"],
            zeitlicher_bezug=["2021-09 bis 2021-10"],
            rechte="Testrechte",
            studienbezug=["1234567"],
            alternativer_titel="alternativer Testitel",
            anonymisiert_pseudonymisiert="pseudonymisiert",
            externe_partner="esterner Testpartner",
            id_loinc=["12345-6"],
            id_mesh_begriff=["D123"],
            kontakt=["resolvedr@rki.de"],
            methodenbeschreibung=["Testmethodenbeschreibung"],
            mitwirkende_fachabteilung="mitwirkende Testabteilung",
            mitwirkende_personen="mitwirkende Testperson",
            raeumlicher_bezug=["räumlicher Testbezug"],
            ressourcentyp_allgemein="allgemeiner Testtyp",
            ressourcentyp_speziell=["spezieller Testtyp"],
            sheet_name="Probe1",
            thema=["https://mex.rki.de/item/theme-1"],
            tools_instrumente_oder_apparate="Testtool",
            verantwortliche_fachabteilung="PARENT Dept.",
            verwandte_publikation_doi="testverwandedoi",
            verwandte_publikation_titel="testverwandtepublikation",
            vorhandene_anzahl_der_proben="Testanzahl",
            weiterfuehrende_dokumentation_titel="Testdokutitel",
            weiterfuehrende_dokumentation_url_oder_dateipfad="Testdokupfad",
            zugriffsbeschraenkung="restriktiv",
        )
    ]


@pytest.fixture
def resource_mapping(settings: Settings) -> ResourceMapping:
    """Mock resource mapping."""
    return ResourceMapping.model_validate(
        load_yaml(settings.biospecimen.mapping_path / "resource_mock.yaml")
    )
