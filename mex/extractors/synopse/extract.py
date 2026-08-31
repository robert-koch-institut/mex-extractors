from collections import defaultdict
from io import BytesIO
from typing import TYPE_CHECKING, Any

import pandas as pd
from pydantic import ValidationError

from mex.common.ldap.transform import analyse_person_string
from mex.common.logging import logger
from mex.common.models import BaseModel
from mex.extractors.assets.helpers import read_bytes
from mex.extractors.ldap.helpers import (
    get_ldap_merged_contact_id_by_mail,
    get_ldap_merged_person_id_by_query,
)
from mex.extractors.logging import watch_progress
from mex.extractors.settings import ExtractorsSettings
from mex.extractors.synopse.models.project import ProjektUndStudienverwaltung
from mex.extractors.synopse.models.study import MetadatenZuDatensaetzen
from mex.extractors.synopse.models.study_overview import Datensatzuebersicht
from mex.extractors.synopse.models.variable import Variablenuebersicht
from mex.extractors.utils import get_dtypes_for_model
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
)

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable

    from mex.common.models import AccessPlatformMapping
    from mex.common.types import (
        MergedContactPointIdentifier,
        MergedOrganizationIdentifier,
        MergedPersonIdentifier,
    )


def parse_csv[BaseModelT: BaseModel](  # noqa: C901
    path: str,
    into: type[BaseModelT],
    chunksize: int = 10000,
    summary_batch_size: int = 10000,
    **kwargs: Any,  # noqa: ANN401
) -> Generator[BaseModelT]:
    """Parse a CSV file into an iterable of the given model type.

    Args:
        path: Location of CSV file
        into: Type of model to parse
        chunksize: Buffer size for chunked reading
        summary_batch_size: Batch size for summary logs
        kwargs: Additional keywords arguments for pandas

    Returns:
        Generator for models
    """
    error_summary: defaultdict[str, int] = defaultdict(int)
    total_rows_processed = 0
    total_rows_successfully_processed = 0
    csv_bytes = read_bytes(path)
    with pd.read_csv(
        BytesIO(csv_bytes),
        chunksize=chunksize,
        dtype=get_dtypes_for_model(into),
        **kwargs,
    ) as reader:
        for i, chunk in enumerate(reader):
            logger.info(
                "parse_csv - %s chunk %s - OK",
                into.__name__,
                i,
            )
            for _, row in chunk.iterrows():
                row_dict = row.to_dict()
                for k, v in row_dict.items():
                    if pd.isna(v) or (isinstance(v, str) and not v.strip()):
                        row_dict[k] = None
                try:
                    model = into.model_validate(row_dict)
                    total_rows_successfully_processed += 1
                    yield model
                except ValidationError as error:
                    for validation_error in error.errors():
                        error_type = validation_error["type"]
                        error_summary[error_type] += 1

                total_rows_processed += 1

                if total_rows_processed % summary_batch_size == 0 and error_summary:
                    logger.error(
                        "Summarizing errors for batch with rows %s to %s",
                        total_rows_processed - summary_batch_size + 1,
                        total_rows_processed,
                    )
                    for error_type, count in error_summary.items():
                        logger.error(
                            " - Error type '%s': %s occurrences", error_type, count
                        )
                    error_summary.clear()

    if error_summary:
        logger.error("Summarizing errors for remaining rows")
        for error_type, count in error_summary.items():
            logger.error(" - Error type '%s': %s occurrences", error_type, count)
        logger.info(
            "Successfully processed %s items.", total_rows_successfully_processed
        )


def extract_variables() -> list[Variablenuebersicht]:
    """Extract variables from `variablenuebersicht` report.

    Settings:
        synopse.variablenuebersicht_path: Path to the `variablenuebersicht` file,
                                   relative to `assets_dir`

    Returns:
        list for Synopse Variables
    """
    settings = ExtractorsSettings.get()
    return list(
        parse_csv(
            settings.synopse.variablenuebersicht_path,
            Variablenuebersicht,
            delimiter=",",
        )
    )


def extract_study_data() -> list[MetadatenZuDatensaetzen]:
    """Extract study data from `metadaten_zu_datensaetzen` report.

    Settings:
        synopse.metadaten_zu_datensaetzen_path: Path to the `metadaten_zu_datensaetzen`
          file,  relative to `assets_dir`

    Returns:
        List of Synopse Studies
    """
    settings = ExtractorsSettings.get()
    return list(
        watch_progress(
            parse_csv(
                settings.synopse.metadaten_zu_datensaetzen_path,
                MetadatenZuDatensaetzen,
                delimiter=",",
            ),
            "extract_study_data",
        )
    )


def extract_projects() -> list[ProjektUndStudienverwaltung]:
    """Extract projects from `projekt_und_studienverwaltung` report.

    Settings:
        synopse.projekt_und_studienverwaltung_path: Path to the
          `projekt_und_studienverwaltung` file,  relative to `assets_dir`

    Returns:
        List of Synopse Projects
    """
    settings = ExtractorsSettings.get()
    return list(
        watch_progress(
            parse_csv(
                settings.synopse.projekt_und_studienverwaltung_path,
                ProjektUndStudienverwaltung,
                delimiter=",",
            ),
            "extract_projects",
        )
    )


def extract_synopse_project_contributor_ids_by_query(
    synopse_projects: Iterable[ProjektUndStudienverwaltung],
) -> dict[str, list[MergedPersonIdentifier]]:
    """Extract Merged persons for Synopse project contributors.

    Args:
        synopse_projects: Synopse projects

    Returns:
        dictionary of Merged person IDs by query string
    """
    seen = set()
    merged_person_ids_by_query: dict[str, list[MergedPersonIdentifier]] = {}
    for project in watch_progress(
        synopse_projects, "extract_synopse_project_contributor_ids_by_query"
    ):
        names = project.beitragende
        if names is None or names in seen:
            continue
        seen.add(names)
        collected_ids = [
            person_id
            for name in analyse_person_string(names)
            if (
                person_id := get_ldap_merged_person_id_by_query(
                    surname=name.surname, given_name=name.given_name
                )
            )
        ]
        merged_person_ids_by_query[names] = collected_ids
    return merged_person_ids_by_query


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


def extract_study_overviews() -> list[Datensatzuebersicht]:
    """Extract projects from `datensatzuebersicht` report.

    Settings:
        synopse.datensatzuebersicht_path: Path to the `datensatzuebersicht` file,
                                   relative to `assets_dir`

    Returns:
        List of Synopse Overviews
    """
    settings = ExtractorsSettings.get()
    return list(
        watch_progress(
            parse_csv(
                settings.synopse.datensatzuebersicht_path,
                Datensatzuebersicht,
                delimiter=",",
            ),
            "extract_study_overviews",
        )
    )


def extract_synopse_organizations(
    synopse_projects: list[ProjektUndStudienverwaltung],
) -> dict[str, MergedOrganizationIdentifier]:
    """Search and extract organization from wikidata.

    Args:
        synopse_projects: list of synopse projects

    Returns:
        Dict with organization label and WikidataOrganization
    """
    synopse_organizations = {
        project.partner_extern for project in synopse_projects
    }.union(
        {
            project.auftraggeber.split("(")[0]
            for project in synopse_projects
            if project.auftraggeber
        }
    )
    return {
        org_name: org_id
        for org_name in synopse_organizations
        if org_name
        and (org_id := get_wikidata_extracted_organization_id_by_name(org_name))
    }
