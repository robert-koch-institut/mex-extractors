from collections.abc import Generator, Iterable

from mex.common.extract import parse_csv
from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models.person import LDAPPersonWithQuery
from mex.common.ldap.transform import analyse_person_string
from mex.common.logging import watch
from mex.common.wikidata.extract import search_organization_by_label
from mex.common.wikidata.models.organization import WikidataOrganization
from mex.extractors.settings import Settings
from mex.extractors.synopse.models.project import SynopseProject
from mex.extractors.synopse.models.study import SynopseStudy
from mex.extractors.synopse.models.study_overview import SynopseStudyOverview
from mex.extractors.synopse.models.variable import SynopseVariable


@watch
def extract_variables() -> Generator[SynopseVariable, None, None]:
    """Extract variables from `variablenuebersicht` report.

    Settings:
        synopse.variablenuebersicht_path: Path to the `variablenuebersicht` file,
                                  absolute or relative to `assets_dir`

    Returns:
        Generator for Synopse Variables
    """
    settings = Settings.get()
    yield from parse_csv(
        settings.synopse.variablenuebersicht_path,
        SynopseVariable,
    )


@watch
def extract_study_data() -> Generator[SynopseStudy, None, None]:
    """Extract study data from `metadaten_zu_datensaetzen` report.

    Settings:
        synopse.metadaten_zu_datensaetzen_path: Path to the `metadaten_zu_datensaetzen`
          file, absolute or relative to `assets_dir`

    Returns:
        Generator for Synopse Studies
    """
    settings = Settings.get()
    yield from parse_csv(settings.synopse.metadaten_zu_datensaetzen_path, SynopseStudy)


@watch
def extract_projects() -> Generator[SynopseProject, None, None]:
    """Extract projects from `projekt_und_studienverwaltung` report.

    Settings:
        synopse.projekt_und_studienverwaltung_path: Path to the
          `projekt_und_studienverwaltung` file, absolute or relative to `assets_dir`

    Returns:
        Generator for Synopse Projects
    """
    settings = Settings.get()
    yield from parse_csv(
        settings.synopse.projekt_und_studienverwaltung_path,
        SynopseProject,
    )


@watch
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
    for project in synopse_projects:
        names = project.beitragende
        if names is None or "nicht mehr im RKI" in names or names in seen:
            continue
        seen.add(names)
        for name in analyse_person_string(names):
            persons = list(ldap.get_persons(name.surname, name.given_name))
            if len(persons) == 1 and persons[0].objectGUID:
                yield LDAPPersonWithQuery(person=persons[0], query=names)


@watch
def extract_study_overviews() -> Generator[SynopseStudyOverview, None, None]:
    """Extract projects from `datensatzuebersicht` report.

    Settings:
        synopse.datensatzuebersicht_path: Path to the `datensatzuebersicht` file,
                                  absolute or relative to `assets_dir`

    Returns:
        Generator for Synopse Overviews
    """
    settings = Settings.get()
    yield from parse_csv(
        settings.synopse.datensatzuebersicht_path, SynopseStudyOverview
    )


def extract_synopse_organizations(
    synopse_projects: list[SynopseProject],
) -> dict[str, WikidataOrganization]:
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
        org_name: org
        for org_name in synopse_organizations
        if org_name and (org := search_organization_by_label(org_name))
    }
