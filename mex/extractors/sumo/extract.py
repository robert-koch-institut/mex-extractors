from collections.abc import Iterable

import numpy as np
from pandas import ExcelFile

from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models import LDAPFunctionalAccount, LDAPPersonWithQuery
from mex.common.ldap.transform import analyse_person_string
from mex.common.models import AccessPlatformMapping, ResourceMapping
from mex.extractors.settings import Settings
from mex.extractors.sumo.models.cc1_data_model_nokeda import Cc1DataModelNoKeda
from mex.extractors.sumo.models.cc1_data_valuesets import Cc1DataValuesets
from mex.extractors.sumo.models.cc2_aux_mapping import Cc2AuxMapping
from mex.extractors.sumo.models.cc2_aux_model import Cc2AuxModel
from mex.extractors.sumo.models.cc2_aux_valuesets import Cc2AuxValuesets
from mex.extractors.sumo.models.cc2_feat_projection import Cc2FeatProjection


def extract_cc1_data_valuesets() -> list[Cc1DataValuesets]:
    """Extract data from cc1_data_valuesets_v3.0.3.xlsx file.

    Settings:
        sumo.raw_data_path: path to directory holding sumo raw data.

    Returns:
        List of cc1_data_valuesets instances
    """
    settings = Settings.get()
    excel_path = settings.sumo.raw_data_path / "cc1_data_valuesets_v3.0.3.xlsx"
    excel_file = ExcelFile(excel_path)
    valuesets = []
    for sheet_name in excel_file.sheet_names:
        data_valuesets = excel_file.parse(sheet_name=sheet_name)
        for _, row in data_valuesets.iterrows():
            valuesets.append(
                Cc1DataValuesets(
                    **row.replace(
                        to_replace=np.nan,
                        value=None,
                    ).replace(
                        regex=r"^\s*$",
                        value=None,
                    ),
                    sheet_name=sheet_name,
                )
            )
    return valuesets


def extract_cc1_data_model_nokeda() -> list[Cc1DataModelNoKeda]:
    """Extract data from cc1_data_model_NoKeda_v3.0.3.xlsx file.

    Settings:
        sumo.raw_data_path: path to directory holding sumo raw data.

    Returns:
        List of Cc1DataModelNoKeda instances
    """
    settings = Settings.get()
    excel_path = settings.sumo.raw_data_path / "cc1_data_model_NoKeda_v3.0.3.xlsx"
    excel_file = ExcelFile(excel_path)
    sheet_name = "datamodel NoKeda"
    data_model_nokeda = excel_file.parse(sheet_name=sheet_name)
    models = []
    for _, row in data_model_nokeda.iterrows():
        models.append(Cc1DataModelNoKeda(**row))
    return models


def extract_cc2_aux_model() -> list[Cc2AuxModel]:
    """Extract data from cc2_aux_model_v3.0.3.xlsx file.

    Settings:
        sumo.raw_data_path: path to directory holding sumo raw data.

    Returns:
        List of cc2_aux_model instances
    """
    settings = Settings.get()
    excel_path = settings.sumo.raw_data_path / "cc2_aux_model_v3.0.3.xlsx"
    excel_file = ExcelFile(excel_path)
    sheet_name = "datamodel aux"
    aux_model = excel_file.parse(sheet_name=sheet_name)
    models = []
    for _, row in aux_model.iterrows():
        models.append(Cc2AuxModel(**row))
    return models


def extract_cc2_aux_mapping(
    sumo_cc2_aux_model: Iterable[Cc2AuxModel],
) -> list[Cc2AuxMapping]:
    """Extract data from cc2_aux_mapping_v3.0.3.xlsx file.

    Args:
        sumo_cc2_aux_model: Cc2AuxModel variables

    Settings:
        sumo.raw_data_path: path to directory holding sumo raw data.

    Returns:
        List of cc2_aux_mapping instances
    """
    settings = Settings.get()
    excel_path = settings.sumo.raw_data_path / "cc2_aux_mapping_v3.0.3.xlsx"
    excel_file = ExcelFile(excel_path)
    mappings = []
    for row in sumo_cc2_aux_model:
        sheet_name = row.depends_on_nokeda_variable
        aux_mapping = excel_file.parse(sheet_name=sheet_name)
        mappings.append(
            Cc2AuxMapping(
                sheet_name=sheet_name,
                column_name=row.variable_name,
                variable_name_column=aux_mapping[row.variable_name].tolist(),
            )
        )
    return mappings


def extract_cc2_aux_valuesets() -> list[Cc2AuxValuesets]:
    """Extract data from cc2_aux_valuesets_v3.0.3.xlsx file.

    Settings:
        sumo.raw_data_path: path to directory holding sumo raw data.

    Returns:
        List of cc2_aux_valuesets instances
    """
    settings = Settings.get()
    excel_path = settings.sumo.raw_data_path / "cc2_aux_valuesets_v3.0.3.xlsx"
    excel_file = ExcelFile(excel_path)
    sheet_name = "aux_cedis_group"
    aux_valuesets = excel_file.parse(sheet_name=sheet_name)
    valuesets = []
    for _, row in aux_valuesets.iterrows():
        valuesets.append(Cc2AuxValuesets(**row))
    return valuesets


def extract_cc2_feat_projection() -> list[Cc2FeatProjection]:
    """Extract data from cc2_feat_projection_v3.0.3.xlsx file.

    Settings:
        sumo.raw_data_path: path to directory holding sumo raw data.

    Returns:
        List of cc2_feat_projection instances
    """
    settings = Settings.get()
    excel_path = settings.sumo.raw_data_path / "cc2_feat_projection_v3.0.3.xlsx"
    excel_file = ExcelFile(excel_path)
    sheet_name = "feat_syndrome"
    aux_valuesets = excel_file.parse(sheet_name=sheet_name)
    projections = []
    for _, row in aux_valuesets.iterrows():
        projections.append(Cc2FeatProjection(**row))
    return projections


def extract_ldap_contact_points_by_emails(
    resources: list[ResourceMapping],
) -> list[LDAPFunctionalAccount]:
    """Extract contact points from ldap for email in resource contacts.

    Args:
        resources: list of sumo resource mapping models

    Returns:
        List of ldap actors
    """
    ldap = LDAPConnector.get()
    emails = {r.contact[0].mappingRules[0].forValues[0] for r in resources}  # type: ignore[index]
    accounts = []
    for mail in emails:
        accounts.extend(ldap.get_functional_accounts(mail=mail))
    return accounts


def extract_ldap_contact_points_by_name(
    sumo_access_platform: AccessPlatformMapping,
) -> list[LDAPPersonWithQuery]:
    """Extract contact points from ldap for contact name in Sumo access platform.

    Args:
        sumo_access_platform: SUMO access platform mapping model

    Returns:
        List of ldap persons with query
    """
    ldap = LDAPConnector.get()
    names = sumo_access_platform.contact[0].mappingRules[0].forValues or []
    split_names = [
        split_name for name in names for split_name in analyse_person_string(name)
    ]
    ldap_persons = []
    for split_name in split_names:
        persons = ldap.get_persons(
            surname=split_name.surname, given_name=split_name.given_name, limit=2
        )
        if len(persons) == 1 and persons[0].objectGUID:
            ldap_persons.append(LDAPPersonWithQuery(person=persons[0], query=names))
    return ldap_persons
