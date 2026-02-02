import pytest

from mex.common.types import (
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedPersonIdentifier,
)
from mex.extractors.ldap.helpers import (
    get_ldap_merged_contact_id_by_mail,
    get_ldap_merged_person_id_by_query,
)


@pytest.mark.integration
@pytest.mark.requires_rki_infrastructure
def test_get_ldap_merged_person_id_by_query(
    mocked_merged_organizational_unit_ids: list[MergedOrganizationalUnitIdentifier],
) -> None:
    merged_person_id = get_ldap_merged_person_id_by_query(
        mocked_merged_organizational_unit_ids,
        display_name="Resolved, Roland",
    )
    assert merged_person_id == MergedPersonIdentifier("eXA2Qj5pKmI7HXIgcVqCfz")


@pytest.mark.integration
@pytest.mark.requires_rki_infrastructure
def test_get_ldap_merged_contact_id_by_mail() -> None:
    merged_contact_point_id = get_ldap_merged_contact_id_by_mail(
        mail="resolvedr@rki.de"
    )

    assert merged_contact_point_id == MergedContactPointIdentifier(
        "cMkmnNOoNVAohBA1XLNr9K"
    )
