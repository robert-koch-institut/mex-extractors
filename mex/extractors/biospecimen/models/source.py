from mex.common.models import BaseModel


class BiospecimenResource(BaseModel):
    """Model class for Biospecimen source entities."""

    file_name: str
    sheet_name: str
    zugriffsbeschraenkung: str
    alternativer_titel: str | None
    anonymisiert_pseudonymisiert: str | None
    kontakt: list[str]
    mitwirkende_fachabteilung: str | None
    mitwirkende_personen: str | None
    beschreibung: list[str]
    weiterfuehrende_dokumentation_titel: str | None
    weiterfuehrende_dokumentation_url_oder_dateipfad: str | None
    externe_partner: str | None
    tools_instrumente_oder_apparate: str | None
    schlagworte: list[str]
    id_loinc: list[str]
    id_mesh_begriff: list[str]
    methoden: list[str]
    methodenbeschreibung: list[str]
    verwandte_publikation_titel: str | None
    verwandte_publikation_doi: str | None
    ressourcentyp_allgemein: str | None
    ressourcentyp_speziell: list[str]
    rechte: str | None
    vorhandene_anzahl_der_proben: str | None
    raeumlicher_bezug: list[str]
    zeitlicher_bezug: list[str]
    thema: list[str]
    offizieller_titel_der_probensammlung: list[str]
    verantwortliche_fachabteilung: str
    studienbezug: list[str]
