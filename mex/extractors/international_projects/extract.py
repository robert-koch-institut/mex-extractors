import re
import warnings
from collections.abc import Generator, Iterable
from typing import Any

import pandas as pd

from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models.person import LDAPPersonWithQuery
from mex.common.ldap.transform import analyse_person_string
from mex.common.logging import logger, watch
from mex.common.types import (
    TemporalEntity,
    TemporalEntityPrecision,
    YearMonthDay,
)
from mex.common.wikidata.extract import search_organization_by_label
from mex.common.wikidata.models.organization import WikidataOrganization
from mex.extractors.international_projects.models.source import (
    InternationalProjectsSource,
)
from mex.extractors.settings import Settings


@watch
def extract_international_projects_sources() -> (
    Generator[InternationalProjectsSource, None, None]
):
    """Extract international projects sources by loading data from MS-Excel file.

    Returns:
        Generator for international projects sources
    """
    settings = Settings.get()
    # silence openpyxl warning:
    # `UserWarning: Data Validation extension is not supported and will be removed`
    # we do not use excels data validation functionality
    with warnings.catch_warnings():
        warnings.filterwarnings(
            action="ignore",
            message="Data Validation extension is not supported and will be removed",
            category=UserWarning,
        )
        df = pd.read_excel(
            settings.international_projects.file_path,
            keep_default_na=False,
            parse_dates=True,
            header=1,
        )
    for row in df.iterrows():
        if source := extract_international_projects_source(row[1]):
            yield source


def extract_international_projects_source(
    row: "pd.Series[Any]",
) -> InternationalProjectsSource | None:
    """Extract one international projects source from an xlrd row.

    Args:
        row: xlrd row representing one source
        column_indices: indices by column names

    Returns:
        international projects source, or None
    """
    funding_type = row.get("Funding type")
    project_lead_person = row.get("Project lead (person)")
    project_lead_rki_unit = row.get("Project lead (RKI unit)")
    start_date = get_temporal_entity_from_cell(row.get("Start date DD.MM.YYYY"))
    end_date = get_temporal_entity_from_cell(row.get("End date DD.MM.YYYY"))
    partner_organization = str(
        row.get("Partner organizations (full name and acronym)", "")
    )
    funding_source = str(row.get("Funding source", ""))
    funding_program = row.get("Funding programme")
    rki_internal_project_number = row.get(
        "RKI internal project number (e.g. 1368-2022)"
    )
    project_abbreviation = row.get("Project Abbreviation")
    additional_rki_units = row.get("Additional RKI units involved")
    full_project_name = row.get(
        "Full project name (as in application or officially amended later)"
    )
    activity1 = str(row.get("Activity 1", ""))
    activity2 = str(row.get("Activity 2 (optional)", ""))
    topic1 = str(row.get("Topic 1", ""))
    topic2 = str(row.get("Topic 2 (optional)", ""))
    homepage = row.get("Homepage")

    if not full_project_name or not project_abbreviation:
        return None
    if not project_lead_rki_unit:
        return None

    return InternationalProjectsSource(
        funding_type=funding_type,
        project_lead_person=project_lead_person,
        end_date=end_date,
        partner_organization=get_clean_organizations_names(partner_organization),
        funding_source=funding_source.split("\n"),
        funding_program=funding_program,
        rki_internal_project_number=rki_internal_project_number,
        additional_rki_units=additional_rki_units,
        project_lead_rki_unit=project_lead_rki_unit,
        project_abbreviation=project_abbreviation,
        start_date=start_date,
        activity1=activity1,
        activity2=activity2,
        topic1=topic1,
        topic2=topic2,
        full_project_name=full_project_name,
        website=homepage,
    )


@watch
def extract_international_projects_project_leaders(
    international_projects_sources: Iterable[InternationalProjectsSource],
) -> Generator[LDAPPersonWithQuery, None, None]:
    """Extract LDAP persons with their query string for project leaders.

    Args:
        international_projects_sources: international projects sources

    Returns:
        Generator for LDAP persons with query
    """
    ldap = LDAPConnector.get()
    seen = set()
    for source in international_projects_sources:
        names = source.project_lead_person
        if not names:
            continue
        if names in seen:
            continue
        seen.add(names)
        for name in analyse_person_string(names):
            persons = list(ldap.get_persons(name.surname, name.given_name))
            if len(persons) == 1 and persons[0].objectGUID:
                yield LDAPPersonWithQuery(person=persons[0], query=names)


def extract_international_projects_funding_sources(
    international_projects_sources: Iterable[InternationalProjectsSource],
) -> dict[str, WikidataOrganization]:
    """Search and extract funding organization from wikidata.

    Args:
        international_projects_sources: Iterable of international-project sources

    Returns:
        Dict with organization label and WikidataOrganization
    """
    found_orgs = {}
    for source in international_projects_sources:
        if funder_or_commissioner := source.funding_source:
            for org in funder_or_commissioner:
                if wikidata_org := search_organization_by_label(org):
                    found_orgs[org] = wikidata_org
    return found_orgs


def extract_international_projects_partner_organizations(
    international_projects_sources: Iterable[InternationalProjectsSource],
) -> dict[str, WikidataOrganization]:
    """Search and extract partner organization from wikidata.

    Args:
        international_projects_sources: Iterable of international-project sources

    Returns:
        Dict with organization label and WikidataOrganization
    """
    found_orgs = {}
    for source in international_projects_sources:
        if partner_organizations := source.partner_organization:
            for org in partner_organizations:
                if wikidata_org := search_organization_by_label(org):
                    found_orgs[org] = wikidata_org
    return found_orgs


def get_temporal_entity_from_cell(
    cell_value: Any,
) -> TemporalEntity | YearMonthDay | None:
    """Try to extract a temporal_entity from a cell.

    Args:
        cell_value: Value of a cell, could be int, string or datetime

    Returns:
        TemporalEntity or None
    """
    try:
        return YearMonthDay(cell_value, precision=TemporalEntityPrecision.DAY)
    except (TypeError, ValueError) as error:
        logger.debug(error)
        return None


def get_clean_organizations_names(organizations_str: str) -> list[str]:
    """Get clean names for partner organizations.

    Args:
        organizations_str (str): string containing all organizations names

    Returns:
        list of clean organizations names
    """
    organizations_str = (
        organizations_str.replace(",,", ",").replace("»", "").replace("•", "")
    )
    unclean_organizations = organizations_str.split("\n")
    clean_organizations = []
    for org in unclean_organizations:
        if org:
            org = re.sub("^[0-9]+", "", org)
            org = org.strip()
            clean_organizations.append(org)

    return clean_organizations
