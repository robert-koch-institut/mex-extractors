from typing import cast

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.models import MergedPerson
from mex.common.types import MergedPrimarySourceIdentifier


def extract_ldap_persons(
    extracted_primary_source_ldap_identifier: MergedPrimarySourceIdentifier,
) -> list[MergedPerson]:
    """Get all persons from primary source LDAP."""
    connector = BackendApiConnector.get()
    container = connector.fetch_merged_items(
        query_string=None,
        entity_type=["MergedPerson"],
        had_primary_source=[extracted_primary_source_ldap_identifier],
        skip=0,
        limit=100,
    )
    return cast("list[MergedPerson]", container.items)
