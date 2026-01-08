import pytest

from mex.common.models import ExtractedOrganizationalUnit
from mex.common.types import MergedContactPointIdentifier, MergedPersonIdentifier
from mex.extractors.ldap.helpers import get_ldap_merged_ids_by_query


@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_get_ldap_merged_ids_by_query(
    mocked_extracted_organizational_units: list[ExtractedOrganizationalUnit],
) -> None:
    merged_ids = get_ldap_merged_ids_by_query(
        "Resolved, Roland", mocked_extracted_organizational_units
    )

    assert merged_ids == [MergedPersonIdentifier("eXA2Qj5pKmI7HXIgcVqCfz")]

    merged_ids = get_ldap_merged_ids_by_query(
        "email@email.de", mocked_extracted_organizational_units
    )

    assert merged_ids == [MergedContactPointIdentifier("cMkmnNOoNVAohBA1XLNr9K")]
