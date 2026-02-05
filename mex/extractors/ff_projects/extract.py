import re
from collections.abc import Iterable
from datetime import datetime
from functools import lru_cache
from typing import Any

import pandas as pd

from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models import LDAPPersonWithQuery
from mex.common.ldap.transform import analyse_person_string
from mex.common.types import (
    MergedOrganizationIdentifier,
    TemporalEntity,
    TemporalEntityPrecision,
)
from mex.extractors.ff_projects.models.source import FFProjectsSource
from mex.extractors.logging import watch_progress
from mex.extractors.settings import Settings
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
)


def extract_ff_projects_sources() -> list[FFProjectsSource]:
    """Extract FF Projects sources by loading data from MS-Excel file.

    Settings:
        ff_projects.file_path: Path to the ff-projects list, absolute or relative to
          `assets_dir`

    Returns:
        List of FF Projects sources
    """
    settings = Settings.get()
    ff_projects_excel = pd.read_excel(
        settings.ff_projects.file_path,
        keep_default_na=False,
        parse_dates=True,
    )
    return [
        source
        for row in watch_progress(
            ff_projects_excel.iterrows(), "extract_ff_projects_sources"
        )
        if (source := extract_ff_projects_source(row[1]))
    ]


def get_temporal_entity_from_cell(cell_value: Any) -> TemporalEntity | None:  # noqa: ANN401
    """Try to extract a temporal_entity from a cell.

    Args:
        cell_value: Value of a cell, could be int, string or datetime

    Returns:
        TemporalEntity or None
    """
    if isinstance(cell_value, datetime):
        temporal_entity = TemporalEntity(cell_value)
        temporal_entity.precision = (
            TemporalEntityPrecision.SECOND
        )  # keeps TemporalEntity precision in Seconds as standard.
        return temporal_entity
    return None


def get_string_from_cell(cell_value: Any) -> str:  # noqa: ANN401
    """Try to extract the string value from a cell by truncating floats.

    Args:
        cell_value: Value of a cell, could be string, int or datetime

    Returns:
        String
    """
    if string := str(cell_value).strip():
        return " ".join(string.split())  # normalize whitespaces
    return str(cell_value)


def get_optional_string_from_cell(cell_value: Any) -> str | None:  # noqa: ANN401
    """Try to extract the string value from a cell by truncating floats.

    Args:
        cell_value: Value of a cell, could be string, int or datetime

    Returns:
        String or None
    """
    if string := str(cell_value).strip():
        return " ".join(string.split())  # normalize whitespaces
    return str(cell_value) if cell_value else None


def extract_ff_projects_source(row: "pd.Series[Any]") -> FFProjectsSource | None:
    """Extract one FF Projects source from a single pandas series row.

    Args:
        row: pandas df series row representing one source

    Returns:
        FF Projects source
    """
    kategorie = get_optional_string_from_cell(row.get("Kategorie"))
    foerderprogr = get_optional_string_from_cell(
        row.get("Förderprogr.(FP7, H2020 etc.) ab 08/2015")
    )
    thema_des_projekts = get_string_from_cell(row.get("Thema des Projekts"))
    rki_az = get_string_from_cell(row.get("RKI-AZ"))
    laufzeit_von_cell = row.get("Laufzeit:\nvon            ")
    laufzeit_bis_cell = row.get("bis")
    laufzeit_cells = (
        get_optional_string_from_cell(laufzeit_von_cell),
        get_optional_string_from_cell(laufzeit_bis_cell),
    )
    laufzeit_von = get_temporal_entity_from_cell(laufzeit_von_cell)
    laufzeit_bis = get_temporal_entity_from_cell(laufzeit_bis_cell)
    zuwendungs_oder_auftraggeber = str(row.get("Zuwendungs-/ Auftraggeber"))
    lfd_nr = get_string_from_cell(row.get("lfd. Nr."))
    projektleiter = get_string_from_cell(row.get("Projektleiter"))
    rki_oe = get_optional_string_from_cell(row.get("RKI- OE"))

    return FFProjectsSource(
        kategorie=kategorie,
        foerderprogr=foerderprogr,
        thema_des_projekts=thema_des_projekts,
        rki_az=rki_az,
        laufzeit_cells=laufzeit_cells,
        laufzeit_von=laufzeit_von,
        laufzeit_bis=laufzeit_bis,
        projektleiter=projektleiter,
        rki_oe=rki_oe,
        zuwendungs_oder_auftraggeber=zuwendungs_oder_auftraggeber,
        lfd_nr=lfd_nr,
    )


@lru_cache(maxsize=1024)
def get_clean_names(name: str) -> str:
    """Clean name from unwanted characters and numerals.

    Args:
        name: Name of the person

    Returns:
        str: Cleaned Name
    """
    name = name.replace("?", "")
    name = name.replace("-", " ")
    name = re.sub(r"[0-9()]", "/", name)
    name = re.sub(r"\/+", "/", name)
    name = name.removesuffix("/")
    return name.strip()


def extract_ff_project_authors(
    ff_projects_sources: Iterable[FFProjectsSource],
) -> list[LDAPPersonWithQuery]:
    """Extract LDAP persons with their query string for FF Projects authors.

    Args:
        ff_projects_sources: FF Projects sources

    Returns:
        List of LDAP persons with query
    """
    ldap = LDAPConnector.get()
    seen = set()
    ldap_persons = []
    for source in watch_progress(ff_projects_sources, "extract_ff_project_authors"):
        names = source.projektleiter
        if not names:
            continue
        if names in seen:
            continue
        seen.add(names)
        clean_names = get_clean_names(names)
        for name in analyse_person_string(clean_names):
            persons = ldap.get_persons(
                surname=name.surname, given_name=name.given_name, limit=2
            )
            if len(persons) == 1 and persons[0].objectGUID:
                ldap_persons.append(LDAPPersonWithQuery(person=persons[0], query=names))
    return ldap_persons


def extract_ff_projects_organizations(
    ff_projects_sources: Iterable[FFProjectsSource],
) -> dict[str, MergedOrganizationIdentifier]:
    """Search and extract organization from wikidata.

    Args:
        ff_projects_sources: Iterable of ff-project sources

    Returns:
        Dict with organization label and WikidataOrganization ID
    """
    return {
        org_name: org_id
        for source in ff_projects_sources
        if source.zuwendungs_oder_auftraggeber
        and source.zuwendungs_oder_auftraggeber != "Sonderforschung"
        for org_name in (
            part.strip() for part in source.zuwendungs_oder_auftraggeber.split("/")
        )
        if (org_id := get_wikidata_extracted_organization_id_by_name(org_name))
    }
