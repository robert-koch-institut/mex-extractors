import pytest

from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedPersonIdentifier,
)
from mex.extractors.ldap.helpers import (
    get_ldap_merged_person_id_by_query,
)


@pytest.mark.integration
def test_get_ldap_merged_person_id_by_query(
    mocked_merged_organizational_unit_ids: list[MergedOrganizationalUnitIdentifier],
) -> None:
    merged_person_id = get_ldap_merged_person_id_by_query(
        mocked_merged_organizational_unit_ids,
        display_name="Resolved, Roland",
    )
    assert merged_person_id == MergedPersonIdentifier("eXA2Qj5pKmI7HXIgcVqCfz")
