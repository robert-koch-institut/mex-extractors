from uuid import UUID

import pytest

from mex.common.ldap.models import LDAPFunctionalAccount
from mex.common.models import AccessPlatformMapping, ResourceMapping
from mex.extractors.igs.extract import (
    extract_endpoint_counts,
    extract_igs_schemas,
    extract_ldap_actors_by_mail,
)
from mex.extractors.igs.model import IGSSchema


@pytest.mark.usefixtures("mocked_igs")
def test_extract_igs_schemas() -> None:
    schemas = extract_igs_schemas()
    assert schemas["igsmodels__enums__Pathogen"].model_dump() == {
        "enum": ["PATHOGEN"],
        "type": "string",
    }
    assert schemas["schemaCreation"].model_dump() == {
        "properties": {
            "schemas": {
                "type": "date",
                "items": {"$ref": "#/components/schemas/Pathogen"},
                "title": "test_title",
            },
        }
    }


@pytest.mark.usefixtures("mocked_igs")
def test_extract_endpoint_counts(
    igs_resource_mapping: ResourceMapping, igs_schemas: dict[str, IGSSchema]
) -> None:
    endpoint_counts = extract_endpoint_counts(igs_resource_mapping, igs_schemas)
    assert endpoint_counts == {
        "/test/count": "7",
        "pathogen_PATHOGEN": "4",
        "/uploads/count": "5",
    }


@pytest.mark.usefixtures("mocked_ldap")
def test_extract_ldap_actors_by_mail(
    igs_resource_mapping: ResourceMapping,
    igs_access_platform_mapping: AccessPlatformMapping,
) -> None:
    ldap_actors = extract_ldap_actors_by_mail(
        igs_resource_mapping, igs_access_platform_mapping
    )
    expected = {
        "contactc@rki.de": LDAPFunctionalAccount(
            sAMAccountName="ContactC",
            objectGUID=UUID("00000000-0000-4000-8000-000000000004"),
            mail=["contactc@rki.de"],
            ou="Funktion",
        ),
    }
    assert ldap_actors == expected
