import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pandas import DataFrame, ExcelFile, Series

from mex.common.exceptions import MExError
from mex.common.ldap.connector import LDAPConnector
from mex.extractors.biospecimen.models.source import BiospecimenResource
from mex.extractors.logging import watch_progress
from mex.extractors.settings import Settings
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

    from mex.common.ldap.models import LDAPPerson
    from mex.common.types import MergedOrganizationIdentifier


def extract_biospecimen_contacts_by_email(
    biospecimen_resource: Iterable[BiospecimenResource],
) -> list[LDAPPerson]:
    """Extract LDAP persons for Biospecimen contacts.

    Args:
        biospecimen_resource: Biospecimen resources

    Returns:
        List of LDAP persons
    """
    ldap = LDAPConnector.get()
    seen = set()
    persons = []
    for resource in watch_progress(
        biospecimen_resource, "extract_biospecimen_contacts_by_email"
    ):
        for kontakt in resource.kontakt:
            if kontakt in seen:
                continue
            try:
                persons.append(ldap.get_person(mail=kontakt))
                seen.add(kontakt)
            except MExError:
                continue
    return persons


def extract_biospecimen_organizations(
    biospecimen_resources: Iterable[BiospecimenResource],
) -> dict[str, MergedOrganizationIdentifier]:
    """Search and extract organization from wikidata.

    Args:
        biospecimen_resources: Iterable of biospecimen resources

    Returns:
        dict with WikidataOrganization ID by externe partner
    """
    return {
        resource.externe_partner: org_id
        for resource in biospecimen_resources
        if resource.externe_partner
        and (
            org_id := get_wikidata_extracted_organization_id_by_name(
                resource.externe_partner
            )
        )
    }


def extract_biospecimen_resources() -> list[BiospecimenResource]:
    """Extract Biospecimen resources by loading data from MS-Excel file.

    Settings:
        dir_path: Path to the biospecimen directory,
                  absolute or relative to `assets_dir`

    Returns:
        List of Biospecimen resources
    """
    settings = Settings.get()
    resources = []
    for file in watch_progress(
        Path(settings.biospecimen.raw_data_path).glob("*.xlsx"),
        "extract_biospecimen_resources",
    ):
        xls = ExcelFile(file)
        sheets = xls.book.worksheets
        for sheet in sheets:
            if sheet.sheet_state == "visible":
                sheet_df = xls.parse(sheet_name=sheet.title, header=1)
                if resource := extract_biospecimen_resource(
                    sheet_df, str(sheet.title), file.name
                ):
                    resources.append(resource)
    return resources


def get_clean_string(series: Series[Any]) -> str:
    """Clean string DataFrame and concatenate to one string.

    Args:
        series: series of related field

    Returns:
        string of extracted field
    """
    series = series.dropna()
    series = series.astype("str")
    series = series.str.replace("\\n", ", ", regex=True)
    series = series.str.replace(" +", " ", regex=True)
    series = series.str.strip(", ")
    return str(series.str.cat(sep=", "))


def get_values(
    resource: DataFrame | None, key_col: str, val_col: str, field_name: str
) -> str | None:
    """Extract values of resource corresponding to Feldname.

    Args:
        resource: Biospecimen resource
        key_col: column in the file with keys
        val_col: column in the file with values
        field_name: column name of extracted field

    Returns:
        string of extracted field
    """
    if resource is not None:
        field_values = resource.loc[resource[key_col] == field_name][val_col]
        if all(field_values.isnull()):  # noqa: PD003
            return None
        return get_clean_string(field_values)
    return None


def get_clean_file_name(file_name: str) -> str:
    """Clean file name string.

    Args:
        file_name: file_name string

    Returns:
        cleaned file name string
    """
    return re.sub(r"[_\-\.]", "", file_name)


def get_year_from_zeitlicher_bezug(
    resource: DataFrame | None, key_col: str, val_col: str, field_name: str
) -> str | None:
    """Extract the first four connected digits of the string as year.

    Args:
        resource: Biospecimen resource
        key_col: column in the file with keys
        val_col: column in the file with values
        field_name: column name of extracted field

    Returns:
        string with first four digits treated as zeitlicher_bezug year
    """
    zeitlicher_bezug = get_values(resource, key_col, val_col, field_name)
    if zeitlicher_bezug is not None:
        match = re.search(r"\d{4}", zeitlicher_bezug)
        if match:
            return match.group(0)
    return None


def extract_biospecimen_resource(
    resource: DataFrame, sheet_name: str, file_name: str
) -> BiospecimenResource | None:
    """Extract one Biospecimen resource from an xlsx file.

    Args:
        resource: DataFrame containing resource information
        sheet_name: Name of the Excel sheet the data came from
        file_name: Name of the Excel file

    Settings:
        key_col: column in the file with keys
        val_col: column in the file with values

    Returns:
        Biospecimen resource
    """
    settings = Settings.get()
    key_col = settings.biospecimen.key_col
    val_col = settings.biospecimen.val_col

    zugriffsbeschraenkung = get_values(
        resource, key_col, val_col, "Zugriffsbeschränkung"
    )
    alternativer_titel = get_values(resource, key_col, val_col, "alternativer Titel")
    anonymisiert_pseudonymisiert = get_values(
        resource, key_col, val_col, "anonymisiert / pseudonymisiert"
    )
    kontakt = resource.loc[resource[key_col] == "Kontakt"][val_col].tolist()
    mitwirkende_fachabteilung = get_values(
        resource, key_col, val_col, "mitwirkende Fachabteilung"
    )
    mitwirkende_personen = get_values(
        resource, key_col, val_col, "Mitwirkende Personen"
    )
    beschreibung = get_values(resource, key_col, val_col, "Beschreibung")
    weiterfuehrende_dokumentation_titel = get_values(
        resource, key_col, val_col, "weiterführende Dokumentation, Titel"
    )
    weiterfuehrende_dokumentation_url_oder_dateipfad = get_values(
        resource,
        key_col,
        val_col,
        "weiterführende Dokumentation, URL oder Dateipfad",
    )
    externe_partner = get_values(resource, key_col, val_col, "externe Partner")
    tools_instrumente_oder_apparate = get_values(
        resource, key_col, val_col, "Tools, Instrumente oder Apparate"
    )
    schlagworte = get_values(resource, key_col, val_col, "Schlagworte")
    id_loinc = get_values(resource, key_col, val_col, "ID LOINC")
    id_mesh_begriff = resource.loc[resource[key_col] == "ID MeSH-Begriff"][
        val_col
    ].tolist()
    methoden = get_values(resource, key_col, val_col, "Methode(n)")
    methodenbeschreibung = get_values(
        resource, key_col, val_col, "Methodenbeschreibung"
    )
    verwandte_publikation_titel = get_values(
        resource, key_col, val_col, "Verwandte Publikation, Titel"
    )
    verwandte_publikation_doi = get_values(
        resource, key_col, val_col, "Verwandte Publikation, DOI"
    )
    ressourcentyp_allgemein = get_values(
        resource, key_col, val_col, "Ressourcentyp, allgemein"
    )
    ressourcentyp_speziell = get_values(
        resource, key_col, val_col, "Ressourcentyp, speziell"
    )
    rechte = get_values(resource, key_col, val_col, "Rechte")
    vorhandene_anzahl_der_proben = get_values(
        resource, key_col, val_col, "Vorhandene Anzahl der Proben"
    )
    raeumlicher_bezug = get_values(resource, key_col, val_col, "räumlicher Bezug")
    zeitlicher_bezug = get_values(resource, key_col, val_col, "zeitlicher Bezug")
    thema = get_values(resource, key_col, val_col, "Thema ")
    offizieller_titel_der_probensammlung = get_values(
        resource, key_col, val_col, "offizieller Titel der Probensammlung"
    )
    verantwortliche_fachabteilung = get_values(
        resource, key_col, val_col, "verantwortliche Fachabteilung"
    )
    studienbezug = get_values(resource, key_col, val_col, "Studienbezug")
    sheet_name = get_clean_file_name(str(sheet_name))

    return BiospecimenResource(
        file_name=file_name,
        sheet_name=sheet_name,
        zugriffsbeschraenkung=zugriffsbeschraenkung,
        alternativer_titel=alternativer_titel,
        anonymisiert_pseudonymisiert=anonymisiert_pseudonymisiert,
        kontakt=kontakt,
        mitwirkende_fachabteilung=mitwirkende_fachabteilung,
        mitwirkende_personen=mitwirkende_personen,
        beschreibung=beschreibung,
        weiterfuehrende_dokumentation_titel=weiterfuehrende_dokumentation_titel,
        weiterfuehrende_dokumentation_url_oder_dateipfad=weiterfuehrende_dokumentation_url_oder_dateipfad,
        externe_partner=externe_partner,
        tools_instrumente_oder_apparate=tools_instrumente_oder_apparate,
        schlagworte=schlagworte,
        id_loinc=id_loinc,
        id_mesh_begriff=id_mesh_begriff,
        methoden=methoden,
        methodenbeschreibung=methodenbeschreibung,
        verwandte_publikation_titel=verwandte_publikation_titel,
        verwandte_publikation_doi=verwandte_publikation_doi,
        ressourcentyp_allgemein=ressourcentyp_allgemein,
        ressourcentyp_speziell=ressourcentyp_speziell,
        rechte=rechte,
        vorhandene_anzahl_der_proben=vorhandene_anzahl_der_proben,
        raeumlicher_bezug=raeumlicher_bezug,
        zeitlicher_bezug=zeitlicher_bezug,
        thema=thema,
        offizieller_titel_der_probensammlung=offizieller_titel_der_probensammlung,
        verantwortliche_fachabteilung=verantwortliche_fachabteilung,
        studienbezug=studienbezug,
    )
