from typing import TYPE_CHECKING

import pytest

from mex.extractors.voxco.extract import (
    extract_ldap_persons_voxco,
    extract_voxco_organizations,
    extract_voxco_variables,
)

if TYPE_CHECKING:
    from mex.common.models import (
        ExtractedOrganization,
        ExtractedPerson,
        ResourceMapping,
    )


@pytest.mark.usefixtures("mocked_drop")
def test_extract_voxco_variables() -> None:
    sources = extract_voxco_variables()
    expected = {
        "Id": 50614,
        "DataType": "Text",
        "Type": "Discrete",
        "QuestionText": "Monat",
        "Choices": [
            "@{Code=1; Text=Januar; Image=; HasOpenEnd=False; Visible=True; Default=False}",
            "@{Code=1; Text=Februar; Image=; HasOpenEnd=False; Visible=True; Default=False}",
        ],
        "Text": "Tag",
    }
    assert sources["voxco_data"][0].model_dump() == expected


@pytest.mark.usefixtures("mocked_wikidata")
def test_extract_voxco_organizations(
    voxco_resource_mappings: list[ResourceMapping],
    wikidata_extracted_organization: ExtractedOrganization,
) -> None:
    organizations = extract_voxco_organizations(voxco_resource_mappings)
    assert organizations == {
        "Robert Koch-Institut": wikidata_extracted_organization.stableTargetId,
    }


@pytest.mark.usefixtures("mocked_ldap", "mocked_wikidata")
def test_extract_ldap_persons_voxco(
    voxco_resource_mappings: list[ResourceMapping],
    roland_resolved: ExtractedPerson,
) -> None:
    person_ids = extract_ldap_persons_voxco(voxco_resource_mappings)

    assert person_ids == {roland_resolved.email[0]: roland_resolved.stableTargetId}
