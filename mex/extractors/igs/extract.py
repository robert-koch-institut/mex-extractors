from typing import TYPE_CHECKING, cast

from mex.common.ldap.connector import LDAPConnector
from mex.extractors.igs.connector import IGSConnector
from mex.extractors.igs.model import (
    IGSEnumSchema,
    IGSInfo,
    IGSPropertiesSchema,
    IGSSchema,
)

if TYPE_CHECKING:
    from mex.common.ldap.models import LDAPFunctionalAccount
    from mex.common.models import (
        AccessPlatformMapping,
        ResourceMapping,
    )


def extract_igs_info() -> IGSInfo:
    """Extract IGS info.

    Returns:
        IGS info
    """
    connector = IGSConnector.get()
    raw_json = connector.get_json_from_api()
    return IGSInfo(**raw_json.get("info", {}))


def extract_igs_schemas() -> dict[str, IGSSchema]:
    """Extract IGS schemas.

    Returns:
        IGS schemas by name
    """
    connector = IGSConnector.get()
    raw_json = connector.get_json_from_api()
    schemas = raw_json.get("components", {}).get("schemas", {})
    igs_schemas: dict[str, IGSSchema] = {}
    for key, value in schemas.items():
        if "enum" in value:
            igs_schemas[key] = IGSEnumSchema(**value)
        if "properties" in value:
            igs_schemas[key] = IGSPropertiesSchema(**value)
    return igs_schemas


def extract_endpoint_counts(
    igs_resource_mapping: ResourceMapping, igs_schemas: dict[str, IGSSchema]
) -> dict[str, str]:
    """Return IGS endpoint counts."""
    count_endpoints = [
        element.fieldInPrimarySource.split(" ")[0]
        for element in igs_resource_mapping.qualityInformation
        if element.fieldInPrimarySource
    ]
    if igs_resource_mapping.sizeOfDataBasis[0].fieldInPrimarySource:
        count_endpoints.append(
            igs_resource_mapping.sizeOfDataBasis[0].fieldInPrimarySource
        )
    connector = IGSConnector.get()
    pathogens = cast("IGSEnumSchema", igs_schemas["igsmodels__enums__Pathogen"]).enum
    endpoint_counts: dict[str, str] = {}
    for endpoint in count_endpoints:
        if endpoint == "/samples/count":
            for pathogen in pathogens:
                endpoint_counts[f"pathogen_{pathogen}"] = connector.get_endpoint_count(
                    endpoint=endpoint,
                    params={"pathogens": pathogen, "include_deleted": "false"},
                )
        else:
            endpoint_counts[endpoint] = connector.get_endpoint_count(endpoint=endpoint)
    return endpoint_counts


def extract_ldap_actors_by_mail(
    igs_resource_mapping: ResourceMapping,
    igs_access_platform_mapping: AccessPlatformMapping,
) -> dict[str, LDAPFunctionalAccount]:
    """Extract ldap actors from default values by mail.

    Args:
        igs_resource_mapping: resources default value mapping model
        igs_access_platform_mapping: access platform default value mapping model

    Returns:
        LDAP actors by mail
    """
    connector = LDAPConnector.get()
    mail_list = igs_resource_mapping.contact[0].mappingRules[0].forValues or []
    if ac_contact := igs_access_platform_mapping.contact[0].mappingRules[0].forValues:
        mail_list.extend(ac_contact)

    return {mail: connector.get_functional_account(mail=mail) for mail in mail_list}
