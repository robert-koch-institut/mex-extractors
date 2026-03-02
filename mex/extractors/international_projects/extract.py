import re
import warnings
from typing import TYPE_CHECKING, Any

import pandas as pd

from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models import LDAPPersonWithQuery
from mex.common.ldap.transform import analyse_person_string
from mex.common.logging import logger
from mex.common.types import (
    MergedOrganizationIdentifier,
    TemporalEntity,
    TemporalEntityPrecision,
    YearMonthDay,
)
from mex.extractors.international_projects.models.source import (
    InternationalProjectsSource,
)
from mex.extractors.logging import watch_progress
from mex.extractors.settings import Settings
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
)

if TYPE_CHECKING:
    from collections.abc import Iterable


def extract_international_projects_sources() -> list[InternationalProjectsSource]:
    """Extract international projects sources by loading data from MS-Excel file.

    Returns:
        list for international projects sources
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
        international_projects_excel = pd.read_excel(
            settings.international_projects.file_path,
            keep_default_na=False,
            parse_dates=True,
            header=0,
            sheet_name="Projects",
        )
    return [
        source
        for row in international_projects_excel.iterrows()
        if (source := extract_international_projects_source(row[1]))
    ]


def extract_international_projects_source(
    row: pd.Series[Any],
) -> InternationalProjectsSource | None:
    """Extract one international projects source from an xlrd row.

    Args:
        row: xlrd row representing one source
        column_indices: indices by column names

    Returns:
        international projects source, or None
    """
    funding_type = str(row.get("Funding type"))
    project_lead_person = str(row.get("Project lead (person)"))
    project_lead_rki_unit = row.get("Project lead (RKI unit)")
    start_date = get_temporal_entity_from_cell(row.get("Start date DD.MM.YYYY"))
    end_date = get_temporal_entity_from_cell(row.get("End date DD.MM.YYYY"))
    partner_organization = str(
        row.get("Partner organizations (full name and acronym)", "")
    )
    funding_source = str(row.get("Funding source", ""))
    funding_program = str(row.get("Funding programme"))
    rki_internal_project_number = str(
        row.get("RKI internal project number (e.g. 1368-2022)")
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
    homepage = str(row.get("Homepage"))

    if not full_project_name or not project_abbreviation:
        return None
    if not project_lead_rki_unit:
        return None

    return InternationalProjectsSource(
        funding_type=funding_type,
        project_lead_person=project_lead_person,
        end_date=end_date,
        partner_organization=get_clean_organizations_names(partner_organization),
        funding_source=funding_source,
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


def extract_international_projects_project_leaders(
    international_projects_sources: Iterable[InternationalProjectsSource],
) -> list[LDAPPersonWithQuery]:
    """Extract LDAP persons with their query string for project leaders.

    Args:
        international_projects_sources: international projects sources

    Returns:
        List of LDAP persons with query
    """
    ldap = LDAPConnector.get()
    seen = set()
    ldap_persons = []
    for source in watch_progress(
        international_projects_sources, "extract_international_projects_project_leaders"
    ):
        names = source.get_project_lead_persons()
        if not names:
            continue

        for name in names:
            if name in seen:
                continue
            seen.add(name)
            for analysed_name in analyse_person_string(name):
                persons = ldap.get_persons(
                    surname=analysed_name.surname,
                    given_name=analysed_name.given_name,
                    limit=2,
                )
                if len(persons) == 1 and persons[0].objectGUID:
                    ldap_persons.append(
                        LDAPPersonWithQuery(person=persons[0], query=name)
                    )
    return ldap_persons


def extract_international_projects_funding_sources(
    international_projects_sources: Iterable[InternationalProjectsSource],
) -> dict[str, MergedOrganizationIdentifier]:
    """Search and extract funding organization from wikidata.

    Args:
        international_projects_sources: Iterable of international-project sources

    Returns:
        Dict with organization label and WikidataOrganization
    """
    found_orgs = {}
    for source in international_projects_sources:
        if funder_or_commissioner := source.get_funding_sources():
            for org in funder_or_commissioner:
                if wikidata_org_id := get_wikidata_extracted_organization_id_by_name(
                    org
                ):
                    found_orgs[org] = wikidata_org_id
    return found_orgs


def extract_international_projects_partner_organizations(
    international_projects_sources: Iterable[InternationalProjectsSource],
) -> dict[str, MergedOrganizationIdentifier]:
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
                if wikidata_org_id := get_wikidata_extracted_organization_id_by_name(
                    org
                ):
                    found_orgs[org] = wikidata_org_id
    return found_orgs


def get_temporal_entity_from_cell(
    cell_value: Any,  # noqa: ANN401
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
        organizations_str.replace(",,", ",")
        .replace("»", "")
        .replace("•", "")
        .replace("...", "")
        .replace("…", "")
    )
    unclean_organizations = organizations_str.split("\n")
    return [
        re.sub("^[0-9]+", "", organization).strip()
        for organization in unclean_organizations
        if organization
    ]
