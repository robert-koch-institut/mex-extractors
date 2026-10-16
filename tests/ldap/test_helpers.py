from typing import TYPE_CHECKING

import pytest

from mex.extractors.ldap.helpers import (
    get_ldap_merged_contact_id_by_mail,
    get_ldap_merged_person_id_by_query,
)

if TYPE_CHECKING:
    from mex.common.models import ExtractedContactPoint, ExtractedPerson


@pytest.mark.requires_rki_infrastructure
@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_get_ldap_merged_person_id_by_query(
    roland_resolved: ExtractedPerson,
) -> None:
    merged_person_id = get_ldap_merged_person_id_by_query(
        display_name="Resolved, Roland",
    )
    assert merged_person_id == roland_resolved.stableTargetId


@pytest.mark.requires_rki_infrastructure
@pytest.mark.usefixtures("mocked_ldap")
def test_get_ldap_merged_contact_id_by_mail(
    contact_point: ExtractedContactPoint,
) -> None:
    merged_contact_point_id = get_ldap_merged_contact_id_by_mail(
        mail="resolvedr@rki.de"
    )

    assert merged_contact_point_id == contact_point.stableTargetId
