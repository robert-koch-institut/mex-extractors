from collections.abc import Generator, Iterable

import numpy as np
from pandas import ExcelFile

from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models import LDAPActor, LDAPPersonWithQuery
from mex.common.ldap.transform import analyse_person_string
from mex.common.models import AccessPlatformMapping, ResourceMapping
from mex.extractors.settings import Settings
from mex.extractors.sumo.models.cc1_data_model_nokeda import Cc1DataModelNoKeda
from mex.extractors.sumo.models.cc1_data_valuesets import Cc1DataValuesets
from mex.extractors.sumo.models.cc2_aux_mapping import Cc2AuxMapping
from mex.extractors.sumo.models.cc2_aux_model import Cc2AuxModel
from mex.extractors.sumo.models.cc2_aux_valuesets import Cc2AuxValuesets
from mex.extractors.sumo.models.cc2_feat_projection import Cc2FeatProjection


def extract_cc1_data_valuesets() -> Generator[Cc1DataValuesets, None, None]:
    """Extract data from cc1_data_valuesets_v3.0.3.xlsx file.

    Settings:
        sumo.raw_data_path: path to directory holding sumo raw data.

    Returns:
        Generator of cc1_data_valuesets instances
    """
    settings = Settings.get()
    excel_path = settings.sumo.raw_data_path / "cc1_data_valuesets_v3.0.3.xlsx"
    excel_file = ExcelFile(excel_path)
    for sheet_name in excel_file.sheet_names:
        data_valuesets = excel_file.parse(sheet_name=sheet_name)
        for _, row in data_valuesets.iterrows():
            yield Cc1DataValuesets(
                **row.replace(
                    to_replace=np.nan,
                    value=None,
                ).replace(
                    regex=r"^\s*$",
                    value=None,
                ),
                sheet_name=sheet_name,
            )


def extract_cc1_data_model_nokeda() -> Generator[Cc1DataModelNoKeda, None, None]:
    """Extract data from cc1_data_model_NoKeda_v3.0.3.xlsx file.

    Settings:
        sumo.raw_data_path: path to directory holding sumo raw data.

    Returns:
        Generator of Cc1DataModelNoKeda instances
    """
    settings = Settings.get()
    excel_path = settings.sumo.raw_data_path / "cc1_data_model_NoKeda_v3.0.3.xlsx"
    excel_file = ExcelFile(excel_path)
    sheet_name = "datamodel NoKeda"
    data_model_nokeda = excel_file.parse(sheet_name=sheet_name)
    for _, row in data_model_nokeda.iterrows():
        yield Cc1DataModelNoKeda(**row)


def extract_cc2_aux_model() -> Generator[Cc2AuxModel, None, None]:
    """Extract data from cc2_aux_model_v3.0.3.xlsx file.

    Settings:
        sumo.raw_data_path: path to directory holding sumo raw data.

    Returns:
        Generator of cc2_aux_model instances
    """
    settings = Settings.get()
    excel_path = settings.sumo.raw_data_path / "cc2_aux_model_v3.0.3.xlsx"
    excel_file = ExcelFile(excel_path)
    sheet_name = "datamodel aux"
    aux_model = excel_file.parse(sheet_name=sheet_name)
    for _, row in aux_model.iterrows():
        yield Cc2AuxModel(**row)


def extract_cc2_aux_mapping(
    sumo_cc2_aux_model: Iterable[Cc2AuxModel],
) -> Generator[Cc2AuxMapping, None, None]:
    """Extract data from cc2_aux_mapping_v3.0.3.xlsx file.

    Args:
        sumo_cc2_aux_model: Cc2AuxModel variables

    Settings:
        sumo.raw_data_path: path to directory holding sumo raw data.

    Returns:
        Generator of cc2_aux_mapping instances
    """
    settings = Settings.get()
    excel_path = settings.sumo.raw_data_path / "cc2_aux_mapping_v3.0.3.xlsx"
    excel_file = ExcelFile(excel_path)
    for row in sumo_cc2_aux_model:
        sheet_name = row.depends_on_nokeda_variable
        aux_mapping = excel_file.parse(sheet_name=sheet_name)
        yield Cc2AuxMapping(
            sheet_name=sheet_name,
            column_name=row.variable_name,
            variable_name_column=aux_mapping[row.variable_name].tolist(),
        )


def extract_cc2_aux_valuesets() -> Generator[Cc2AuxValuesets, None, None]:
    """Extract data from cc2_aux_valuesets_v3.0.3.xlsx file.

    Settings:
        sumo.raw_data_path: path to directory holding sumo raw data.

    Returns:
        Generator of cc2_aux_valuesets instances
    """
    settings = Settings.get()
    excel_path = settings.sumo.raw_data_path / "cc2_aux_valuesets_v3.0.3.xlsx"
    excel_file = ExcelFile(excel_path)
    sheet_name = "aux_cedis_group"
    aux_valuesets = excel_file.parse(sheet_name=sheet_name)
    for _, row in aux_valuesets.iterrows():
        yield Cc2AuxValuesets(**row)


def extract_cc2_feat_projection() -> Generator[Cc2FeatProjection, None, None]:
    """Extract data from cc2_feat_projection_v3.0.3.xlsx file.

    Settings:
        sumo.raw_data_path: path to directory holding sumo raw data.

    Returns:
        Generator of cc2_feat_projection instances
    """
    settings = Settings.get()
    excel_path = settings.sumo.raw_data_path / "cc2_feat_projection_v3.0.3.xlsx"
    excel_file = ExcelFile(excel_path)
    sheet_name = "feat_syndrome"
    aux_valuesets = excel_file.parse(sheet_name=sheet_name)
    for _, row in aux_valuesets.iterrows():
        yield Cc2FeatProjection(**row)


def extract_ldap_contact_points_by_emails(
    resources: list[ResourceMapping],
) -> Generator[LDAPActor, None, None]:
    """Extract contact points from ldap for email in resource contacts.

    Args:
        resources: list of sumo resource mapping models

    Returns:
        Iterable of ldap actors
    """
    ldap = LDAPConnector.get()
    emails = {r.contact[0].mappingRules[0].forValues[0] for r in resources}  # type: ignore[index]
    return (
        actor for mail in emails for actor in ldap.get_functional_accounts(mail=mail)
    )


def extract_ldap_contact_points_by_name(
    sumo_access_platform: AccessPlatformMapping,
) -> Generator[LDAPPersonWithQuery, None, None]:
    """Extract contact points from ldap for contact name in Sumo access platform.

    Args:
        sumo_access_platform: SUMO access platform mapping model

    Returns:
        Iterable of ldap persons with query
    """
    ldap = LDAPConnector.get()
    names = sumo_access_platform.contact[0].mappingRules[0].forValues or []
    split_names = [
        split_name for name in names for split_name in analyse_person_string(name)
    ]
    for split_name in split_names:
        persons = ldap.get_persons(
            surname=split_name.surname, given_name=split_name.given_name, limit=2
        )
        if len(persons) == 1 and persons[0].objectGUID:
            yield LDAPPersonWithQuery(person=persons[0], query=names)
