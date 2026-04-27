from typing import TYPE_CHECKING

from mex.common.extract import parse_csv
from mex.common.ldap.transform import analyse_person_string
from mex.extractors.ldap.helpers import (
    get_ldap_merged_contact_id_by_mail,
    get_ldap_merged_person_id_by_query,
)
from mex.extractors.logging import watch_progress
from mex.extractors.settings import Settings
from mex.extractors.synopse.models.project import SynopseProject
from mex.extractors.synopse.models.study import SynopseStudy
from mex.extractors.synopse.models.study_overview import SynopseStudyOverview
from mex.extractors.synopse.models.variable import SynopseVariable
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
)

if TYPE_CHECKING:
    from collections.abc import Iterable

    from mex.common.models import AccessPlatformMapping
    from mex.common.types import (
        MergedContactPointIdentifier,
        MergedOrganizationIdentifier,
        MergedPersonIdentifier,
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


def extract_study_data() -> list[SynopseStudy]:
    """Extract study data from `metadaten_zu_datensaetzen` report.

    Settings:
        synopse.metadaten_zu_datensaetzen_path: Path to the `metadaten_zu_datensaetzen`
          file, absolute or relative to `assets_dir`

    Returns:
        List of Synopse Studies
    """
    settings = Settings.get()
    return list(
        watch_progress(
            parse_csv(
                settings.synopse.metadaten_zu_datensaetzen_path,
                SynopseStudy,
                delimiter=",",
            ),
            "extract_study_data",
        )
    )


def extract_projects() -> list[SynopseProject]:
    """Extract projects from `projekt_und_studienverwaltung` report.

    Settings:
        synopse.projekt_und_studienverwaltung_path: Path to the
          `projekt_und_studienverwaltung` file, absolute or relative to `assets_dir`

    Returns:
        List of Synopse Projects
    """
    settings = Settings.get()
    return list(
        watch_progress(
            parse_csv(
                settings.synopse.projekt_und_studienverwaltung_path,
                SynopseProject,
                delimiter=",",
            ),
            "extract_projects",
        )
    )


def extract_synopse_project_contributor_ids_by_query(
    synopse_projects: Iterable[SynopseProject],
) -> dict[str, MergedPersonIdentifier]:
    """Extract LDAP persons for Synopse project contributors.

    Args:
        synopse_projects: Synopse projects

    Returns:
        List of LDAP persons
    """
    seen = set()
    ldap_person_ids_by_query: dict[str, MergedPersonIdentifier] = {}
    for project in watch_progress(
        synopse_projects, "extract_synopse_project_contributor_ids_by_query"
    ):
        names = project.beitragende
        if names is None or "nicht mehr im RKI" in names or names in seen:
            continue
        seen.add(names)
        for name in analyse_person_string(names):
            if person_id := get_ldap_merged_person_id_by_query(
                surname=name.surname, given_name=name.given_name
            ):
                ldap_person_ids_by_query[f"{name.surname}\n{name.given_name}"] = (
                    person_id
                )
    return ldap_person_ids_by_query


def extract_synopse_contact(
    access_platform_mapping: AccessPlatformMapping,
) -> dict[str, MergedContactPointIdentifier]:
    """Extract LDAP persons for Synopse project contact.

    Args:
        access_platform_mapping: Synopse access platform default values

    Returns:
        merged contact point id by mail
    """
    contact_list: list[str] = []
    if access_platform_mapping.contact[0].mappingRules[0].forValues:
        contact_list.extend(
            access_platform_mapping.contact[0].mappingRules[0].forValues
        )
    return {
        mail: contact_id
        for mail in contact_list
        if (contact_id := get_ldap_merged_contact_id_by_mail(mail=mail))
    }


def extract_study_overviews() -> list[SynopseStudyOverview]:
    """Extract projects from `datensatzuebersicht` report.

    Settings:
        synopse.datensatzuebersicht_path: Path to the `datensatzuebersicht` file,
                                  absolute or relative to `assets_dir`

    Returns:
        List of Synopse Overviews
    """
    settings = Settings.get()
    return list(
        watch_progress(
            parse_csv(
                settings.synopse.datensatzuebersicht_path,
                SynopseStudyOverview,
                delimiter=",",
            ),
            "extract_study_overviews",
        )
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
