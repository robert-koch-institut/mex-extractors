from uuid import UUID

import pytest

from mex.common.ldap.models import LDAPActor
from mex.common.models import AccessPlatformMapping, ResourceMapping
from mex.extractors.igs.extract import (
    extract_igs_schemas,
    extract_ldap_actors_by_mail,
)


@pytest.mark.usefixtures("mocked_igs")
def test_extract_igs_schemas() -> None:
    schemas = extract_igs_schemas()
    assert schemas["Pathogen"].model_dump() == {"enum": ["PATHOGEN"]}
    assert schemas["schemaCreation"].model_dump() == {
        "properties": {
            "schemas": {
                "type": "date",
                "items": {"$ref": "#/components/schemas/Pathogen"},
                "title": "test_title",
            },
        }
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
        "contactc@rki.de": LDAPActor(
            sAMAccountName="ContactC",
            objectGUID=UUID("00000000-0000-4000-8000-000000000004"),
            mail=["email@email.de", "contactc@rki.de"],
        ),
        "email@email.de": LDAPActor(
            sAMAccountName="ContactC",
            objectGUID=UUID("00000000-0000-4000-8000-000000000004"),
            mail=["email@email.de", "contactc@rki.de"],
        ),
    }
    assert ldap_actors == expected
