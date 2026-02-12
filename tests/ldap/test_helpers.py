import pytest

from mex.common.types import (
    MergedContactPointIdentifier,
    MergedPersonIdentifier,
)
from mex.extractors.ldap.helpers import (
    get_ldap_merged_contact_id_by_mail,
    get_ldap_merged_person_id_by_query,
)


@pytest.mark.requires_rki_infrastructure
@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_get_ldap_merged_person_id_by_query() -> None:
    merged_person_id = get_ldap_merged_person_id_by_query(
        display_name="Resolved, Roland",
    )
    assert merged_person_id == MergedPersonIdentifier("eXA2Qj5pKmI7HXIgcVqCfz")


@pytest.mark.requires_rki_infrastructure
@pytest.mark.usefixtures("mocked_ldap")
def test_get_ldap_merged_contact_id_by_mail() -> None:
    merged_contact_point_id = get_ldap_merged_contact_id_by_mail(
        mail="resolvedr@rki.de"
    )

    assert merged_contact_point_id == MergedContactPointIdentifier(
        "cMkmnNOoNVAohBA1XLNr9K"
    )
