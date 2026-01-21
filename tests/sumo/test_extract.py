from collections.abc import Iterable

import pytest

from mex.common.ldap.models import (
    LDAPFunctionalAccount,
    LDAPPerson,
    LDAPPersonWithQuery,
)
from mex.common.models import (
    AccessPlatformMapping,
    ResourceMapping,
)
from mex.extractors.sumo.extract import (
    extract_cc1_data_model_nokeda,
    extract_cc1_data_valuesets,
    extract_cc2_aux_mapping,
    extract_cc2_aux_model,
    extract_cc2_aux_valuesets,
    extract_cc2_feat_projection,
    extract_ldap_contact_points_by_emails,
    extract_ldap_contact_points_by_name,
)
from mex.extractors.sumo.models.cc1_data_model_nokeda import Cc1DataModelNoKeda
from mex.extractors.sumo.models.cc1_data_valuesets import Cc1DataValuesets
from mex.extractors.sumo.models.cc2_aux_mapping import Cc2AuxMapping
from mex.extractors.sumo.models.cc2_aux_model import Cc2AuxModel
from mex.extractors.sumo.models.cc2_aux_valuesets import Cc2AuxValuesets
from mex.extractors.sumo.models.cc2_feat_projection import Cc2FeatProjection


def test_extract_cc1_data_model_nokeda() -> None:
    expected = Cc1DataModelNoKeda(
        domain="Datenbereitstellung",
        domain_en="data supply",
        type_json="string",
        element_description="shobidoo",
        element_description_en="shobidoo_en",
        variable_name="nokeda_edis_software",
        element_label="Name des EDIS",
        element_label_en="Name of EDIS",
    )
    extracted = extract_cc1_data_model_nokeda()
    assert len(extracted) == 3
    assert extracted[0] == expected


def test_extract_cc1_data_valuesets() -> None:
    expected = Cc1DataValuesets(
        category_label_de="Herzstillstand (nicht traumatisch)",
        category_label_en="Cardiac arrest (non-traumatic)",
        sheet_name="nokeda_cedis",
    )
    extracted = extract_cc1_data_valuesets()
    assert len(extracted) == 6
    assert extracted[0] == expected


def test_extract_cc2_aux_mapping(
    cc2_aux_model: Iterable[Cc2AuxModel],
) -> None:
    expected = Cc2AuxMapping(
        sheet_name="nokeda_age21",
        column_name="aux_age21_min",
        variable_name_column=["0", "1", "2"],
    )
    extracted = extract_cc2_aux_mapping(cc2_aux_model)
    assert len(extracted) == 2
    assert extracted[0] == expected


def test_extract_cc2_aux_model() -> None:
    expected = Cc2AuxModel(
        depends_on_nokeda_variable="nokeda_age21",
        domain="age",
        element_description="the lowest age in the age group",
        in_database_static=True,
        variable_name="aux_age21_min",
    )
    extracted = extract_cc2_aux_model()
    assert len(extracted) == 2
    assert extracted[0] == expected


def test_extract_cc2_aux_valuesets() -> None:
    expected = Cc2AuxValuesets(label_de="Kardiovaskulär", label_en="Cardiovascular")
    extracted = extract_cc2_aux_valuesets()
    assert len(extracted) == 3
    assert extracted[0] == expected


def test_extract_cc2_feat_projection() -> None:
    expected = Cc2FeatProjection(
        feature_abbr="1",
        feature_description="Lorem Ipsum",
        feature_domain="feat_syndrome",
        feature_name_en="Lorem Ipsum",
        feature_name_de="Lorem Ipsum",
        feature_subdomain="RSV",
    )
    extracted = extract_cc2_feat_projection()
    assert len(extracted) == 3
    assert extracted[0] == expected


@pytest.mark.usefixtures("mocked_ldap")
def test_extract_ldap_contact_points_by_emails(
    sumo_resources_feat: ResourceMapping,
    sumo_resources_nokeda: ResourceMapping,
    ldap_contact_point: LDAPFunctionalAccount,
) -> None:
    contact_points = extract_ldap_contact_points_by_emails(
        [sumo_resources_feat, sumo_resources_nokeda]
    )
    assert contact_points == [ldap_contact_point]


@pytest.mark.usefixtures("mocked_ldap")
def test_extract_ldap_contact_points_by_name(
    sumo_access_platform: AccessPlatformMapping,
    ldap_roland_resolved: LDAPPerson,
) -> None:
    sumo_contacts = extract_ldap_contact_points_by_name(sumo_access_platform)
    assert sumo_contacts == [
        LDAPPersonWithQuery(person=ldap_roland_resolved, query="Roland Resolved")
    ]
