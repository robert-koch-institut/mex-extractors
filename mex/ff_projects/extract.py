import re
from collections.abc import Generator, Iterable
from datetime import datetime
from functools import cache
from typing import Any

import pandas as pd

from mex.common.identity import get_provider
from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models.person import LDAPPersonWithQuery
from mex.common.ldap.transform import analyse_person_string
from mex.common.logging import watch
from mex.common.models import ExtractedPrimarySource
from mex.common.types import (
    MergedOrganizationIdentifier,
    TemporalEntity,
    TemporalEntityPrecision,
)
from mex.common.wikidata.extract import search_organization_by_label
from mex.common.wikidata.models.organization import WikidataOrganization
from mex.ff_projects.models.source import FFProjectsSource
from mex.ff_projects.settings import FFProjectsSettings


@watch
def extract_ff_projects_sources() -> Generator[FFProjectsSource, None, None]:
    """Extract FF Projects sources by loading data from MS-Excel file.

    Settings:
        file_path: Path to the ff-projects list, absolute or relative to `assets_dir`

    Returns:
        Generator for FF Projects sources
    """
    settings = FFProjectsSettings.get()
    df = pd.read_excel(
        settings.file_path,
        keep_default_na=False,
        parse_dates=True,
    )
    for row in df.iterrows():
        if source := extract_ff_projects_source(row[1]):
            yield source


def get_timestamp_from_cell(cell_value: Any) -> TemporalEntity | None:
    """Try to extract a timestamp from a cell.

    Args:
        cell_value: Value of a cell, could be int, string or datetime

    Returns:
        TemporalEntity or None
    """
    if isinstance(cell_value, datetime):
        timestamp = TemporalEntity(cell_value)
        timestamp.precision = (
            TemporalEntityPrecision.DAY
        )  # keeps Timestamp precision in Seconds as standard.
        return timestamp
    return None


def get_string_from_cell(cell_value: Any) -> str:
    """Try to extract the string value from a cell by truncating floats.

    Args:
        cell_value: Value of a cell, could be string, int or datetime

    Returns:
        String
    """
    if string := str(cell_value).strip():
        return " ".join(string.split())  # normalize whitespaces
    return str(cell_value)


def get_optional_string_from_cell(cell_value: Any) -> str | None:
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
    laufzeit_von = get_timestamp_from_cell(laufzeit_von_cell)
    laufzeit_bis = get_timestamp_from_cell(laufzeit_bis_cell)
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


def filter_out_duplicate_source_ids(
    sources: Iterable[FFProjectsSource],
) -> Generator[FFProjectsSource, None, None]:
    """Remove duplicate `lfd_nr`s from the given sources.

    Args:
        sources: Iterable of FF Projects sources

    Returns:
        Filtered FF Projects sources
    """
    sources = list(sources)
    lfd_nrs = [source.lfd_nr for source in sources]

    for source in sources:
        if lfd_nrs.count(source.lfd_nr) == 1:
            yield source


@cache
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


@watch
def extract_ff_project_authors(
    ff_projects_sources: Iterable[FFProjectsSource],
) -> Generator[LDAPPersonWithQuery, None, None]:
    """Extract LDAP persons with their query string for FF Projects authors.

    Args:
        ff_projects_sources: FF Projects sources

    Returns:
        Generator for LDAP persons with query
    """
    ldap = LDAPConnector.get()
    seen = set()
    for source in ff_projects_sources:
        names = source.projektleiter
        if not names:
            continue
        if names in seen:
            continue
        seen.add(names)
        clean_names = get_clean_names(names)
        for name in analyse_person_string(clean_names):
            persons = list(ldap.get_persons(name.surname, name.given_name))
            if len(persons) == 1 and persons[0].objectGUID:
                yield LDAPPersonWithQuery(person=persons[0], query=names)


def extract_ff_projects_organizations(
    ff_projects_sources: Iterable[FFProjectsSource],
) -> dict[str, WikidataOrganization]:
    """Search and extract organization from wikidata.

    Args:
        ff_projects_sources: Iterable of ff-project sources

    Returns:
        Dict with organization label and WikidataOrganization
    """
    return {
        source.zuwendungs_oder_auftraggeber: org
        for source in ff_projects_sources
        if source.zuwendungs_oder_auftraggeber
        and (org := search_organization_by_label(source.zuwendungs_oder_auftraggeber))
    }


def get_organization_merged_id_by_query(
    wikidata_organizations_by_query: dict[str, WikidataOrganization],
    wikidata_primary_source: ExtractedPrimarySource,
) -> dict[str, MergedOrganizationIdentifier]:
    """Return a mapping from organizations to their stable target ID.

    There may be multiple entries per unit mapping to the same stable target ID.

    Args:
        wikidata_organizations_by_query: Iterable of extracted organizations
        wikidata_primary_source: Primary source item for wikidata

    Returns:
        Dict with organization label and stable target ID
    """
    identity_provider = get_provider()
    organization_stable_target_id_by_query = {}
    for query, wikidata_organization in wikidata_organizations_by_query.items():
        identities = identity_provider.fetch(
            had_primary_source=wikidata_primary_source.stableTargetId,
            identifier_in_primary_source=wikidata_organization.identifier,
        )
        if identities:
            organization_stable_target_id_by_query[query] = (
                MergedOrganizationIdentifier(identities[0].stableTargetId)
            )

    return organization_stable_target_id_by_query
