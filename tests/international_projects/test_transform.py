from typing import TYPE_CHECKING

import pytest

from mex.common.types import (
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
    TextLanguage,
)
from mex.extractors.international_projects.extract import (
    extract_international_projects_sources,
)
from mex.extractors.international_projects.transform import (
    transform_international_projects_sources_to_extracted_activities,
)
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)

if TYPE_CHECKING:
    from mex.common.models import ActivityMapping, ExtractedOrganizationalUnit


@pytest.mark.usefixtures("mocked_wikidata")
def test_transform_international_projects_source_to_mex_source(
    international_projects_mapping_activity: ActivityMapping,
    mocked_units_by_identifier_in_primary_source: dict[
        str, ExtractedOrganizationalUnit
    ],
) -> None:
    organization_id = MergedOrganizationIdentifier.generate(seed=44)
    funding_source_stable_target_ids_by_synonym = {"Test-Institute": organization_id}
    partner_organizations_stable_target_ids_by_synonym = {"WHO": organization_id}
    person_id = MergedPersonIdentifier.generate(seed=30)
    person_stable_target_ids_by_query_string = {"Dr Frieda Ficticious": [person_id]}

    international_projects_sources = extract_international_projects_sources()

    extracted_activities = (
        transform_international_projects_sources_to_extracted_activities(
            international_projects_sources,
            international_projects_mapping_activity,
            person_stable_target_ids_by_query_string,
            funding_source_stable_target_ids_by_synonym,
            partner_organizations_stable_target_ids_by_synonym,
        )
    )

    expected = {
        "identifier": "f6SlOxfcT1DJVuOnjGqeDl",
        "hadPrimarySource": get_extracted_primary_source_id_by_name(
            "international-projects"
        ),
        "identifierInPrimarySource": "0000-1000",
        "stableTargetId": "gqf9aUfbi297puiQpriwzX",
        "activityType": ["https://mex.rki.de/item/activity-type-1"],
        "alternativeTitle": [{"value": "testAAbr"}],
        "contact": [
            mocked_units_by_identifier_in_primary_source["fg99"].stableTargetId
        ],
        "end": ["2021-12-31"],
        "externalAssociate": [organization_id],
        "funderOrCommissioner": [organization_id],
        "involvedUnit": [
            mocked_units_by_identifier_in_primary_source["fg99"].stableTargetId
        ],
        "responsibleUnit": [
            mocked_units_by_identifier_in_primary_source["fg99"].stableTargetId
        ],
        "shortName": [{"value": "testAAbr"}],
        "start": ["2021-07-27"],
        "theme": ["https://mex.rki.de/item/theme-37"],
        "title": [
            {"value": "This is a test project full title", "language": TextLanguage.EN}
        ],
    }

    assert (
        extracted_activities[0].model_dump(exclude_none=True, exclude_defaults=True)
        == expected
    )
