from typing import cast

from mex.common.logging import logger
from mex.common.models import MergedPerson
from mex.common.types import MergedPrimarySourceIdentifier


def extract_ldap_persons(
    extracted_primary_source_ldap_identifier: MergedPrimarySourceIdentifier,
) -> list[MergedPerson]:
    """Get all persons from primary source LDAP."""
    logger.info(
        "getting persons for %s is not implemented yet",
        extracted_primary_source_ldap_identifier,
    )
    return cast("list[MergedPerson]", [])
