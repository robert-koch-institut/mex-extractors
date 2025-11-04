from collections.abc import Generator, Iterable

from mex.common.extract import parse_csv
from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models import LDAPFunctionalAccount, LDAPPersonWithQuery
from mex.common.ldap.transform import analyse_person_string
from mex.common.models import AccessPlatformMapping
from mex.common.types import MergedOrganizationIdentifier
from mex.extractors.settings import Settings
from mex.extractors.synopse.models.project import SynopseProject
from mex.extractors.synopse.models.study import SynopseStudy
from mex.extractors.synopse.models.study_overview import SynopseStudyOverview
from mex.extractors.synopse.models.variable import SynopseVariable
from mex.extractors.utils import watch_progress
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
)


def extract_variables() -> list[SynopseVariable]:
    """Extract variables from `variablenuebersicht` report.

    Settings:
        synopse.variablenuebersicht_path: Path to the `variablenuebersicht` file,
                                  absolute or relative to `assets_dir`

    Returns:
        list for Synopse Variables
    """
    settings = Settings.get()
    return list(
        parse_csv(
            settings.synopse.variablenuebersicht_path,
            SynopseVariable,
            delimiter=",",
        )
    )


def extract_study_data() -> Generator[SynopseStudy, None, None]:
    """Extract study data from `metadaten_zu_datensaetzen` report.

    Settings:
        synopse.metadaten_zu_datensaetzen_path: Path to the `metadaten_zu_datensaetzen`
          file, absolute or relative to `assets_dir`

    Returns:
        Generator for Synopse Studies
    """
    settings = Settings.get()
    yield from watch_progress(
        parse_csv(
            settings.synopse.metadaten_zu_datensaetzen_path, SynopseStudy, delimiter=","
        ),
        "extract_study_data",
    )


def extract_projects() -> Generator[SynopseProject, None, None]:
    """Extract projects from `projekt_und_studienverwaltung` report.

    Settings:
        synopse.projekt_und_studienverwaltung_path: Path to the
          `projekt_und_studienverwaltung` file, absolute or relative to `assets_dir`

    Returns:
        Generator for Synopse Projects
    """
    settings = Settings.get()
    yield from watch_progress(
        parse_csv(
            settings.synopse.projekt_und_studienverwaltung_path,
            SynopseProject,
            delimiter=",",
        ),
        "extract_projects",
    )


def extract_synopse_project_contributors(
    synopse_projects: Iterable[SynopseProject],
) -> Generator[LDAPPersonWithQuery, None, None]:
    """Extract LDAP persons for Synopse project contributors.

    Args:
        synopse_projects: Synopse projects

    Returns:
        Generator for LDAP persons
    """
    ldap = LDAPConnector.get()
    seen = set()
    for project in watch_progress(
        synopse_projects, "extract_synopse_project_contributors"
    ):
        names = project.beitragende
        if names is None or "nicht mehr im RKI" in names or names in seen:
            continue
        seen.add(names)
        for name in analyse_person_string(names):
            persons = ldap.get_persons(
                surname=name.surname, given_name=name.given_name, limit=2
            )
            if len(persons) == 1 and persons[0].objectGUID:
                yield LDAPPersonWithQuery(person=persons[0], query=names)


def extract_synopse_contact(
    access_platform_mapping: AccessPlatformMapping,
) -> list[LDAPFunctionalAccount]:
    """Extract LDAP persons for Synopse project contact.

    Args:
        access_platform_mapping: Synopse access platform default values

    Returns:
        contact LDAP persons
    """
    ldap = LDAPConnector.get()
    contact_list: list[str] = []
    if access_platform_mapping.contact[0].mappingRules[0].forValues:
        contact_list.extend(
            access_platform_mapping.contact[0].mappingRules[0].forValues
        )
    return [
        account
        for mail in contact_list
        for account in ldap.get_functional_accounts(mail=mail)
    ]


def extract_study_overviews() -> Generator[SynopseStudyOverview, None, None]:
    """Extract projects from `datensatzuebersicht` report.

    Settings:
        synopse.datensatzuebersicht_path: Path to the `datensatzuebersicht` file,
                                  absolute or relative to `assets_dir`

    Returns:
        Generator for Synopse Overviews
    """
    settings = Settings.get()
    yield from watch_progress(
        parse_csv(
            settings.synopse.datensatzuebersicht_path,
            SynopseStudyOverview,
            delimiter=",",
        ),
        "extract_study_overviews",
    )


def extract_synopse_organizations(
    synopse_projects: list[SynopseProject],
) -> dict[str, MergedOrganizationIdentifier]:
    """Search and extract organization from wikidata.

    Args:
        synopse_projects: list of synopse projects

    Returns:
        Dict with organization label and WikidataOrganization
    """
    synopse_organizations = {
        project.externe_partner for project in synopse_projects
    }.union(
        {
            project.foerderinstitution_oder_auftraggeber.split("(")[0]
            for project in synopse_projects
            if project.foerderinstitution_oder_auftraggeber
        }
    )
    return {
        org_name: org_id
        for org_name in synopse_organizations
        if org_name
        and (org_id := get_wikidata_extracted_organization_id_by_name(org_name))
    }
