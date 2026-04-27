from typing import TYPE_CHECKING

import pytest

from mex.common.types import (
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
)
from mex.extractors.international_projects.extract import (
    extract_international_projects_sources,
)
from mex.extractors.international_projects.transform import (
    transform_international_projects_sources_to_extracted_activities,
)

if TYPE_CHECKING:
    from mex.common.models import ActivityMapping


@pytest.mark.usefixtures("mocked_wikidata")
def test_transform_international_projects_source_to_mex_source(
    international_projects_mapping_activity: ActivityMapping,
) -> None:
    organization_id = MergedOrganizationIdentifier.generate(seed=44)
    funding_source_stable_target_ids_by_synonym = {"Test-Institute": organization_id}
    partner_organizations_stable_target_ids_by_synonym = {"WHO": organization_id}
    person_id = MergedPersonIdentifier.generate(seed=30)
    person_stable_target_ids_by_query_string = {"Dr Frieda Ficticious": person_id}

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
        "hadPrimarySource": "fSwk5o6nXHVMdFuPHH0hRk",
        "identifierInPrimarySource": "0000-1000",
        "contact": ["6rqNvZSApUHlz8GkkVP48"],
        "responsibleUnit": ["6rqNvZSApUHlz8GkkVP48"],
        "title": [{"value": "This is a test project full title", "language": "en"}],
        "activityType": ["https://mex.rki.de/item/activity-type-1"],
        "alternativeTitle": [{"value": "testAAbr"}],
        "end": ["2021-12-31"],
        "externalAssociate": ["bFQoRhcVH5DHU8"],
        "funderOrCommissioner": ["bFQoRhcVH5DHU8"],
        "involvedUnit": ["cjna2jitPngp6yIV63cdi9"],
        "shortName": [{"value": "testAAbr"}],
        "start": ["2021-07-27"],
        "theme": ["https://mex.rki.de/item/theme-37"],
        "identifier": "f6SlOxfcT1DJVuOnjGqeDl",
        "stableTargetId": "gqf9aUfbi297puiQpriwzX",
    }

    assert (
        extracted_activities[0].model_dump(exclude_none=True, exclude_defaults=True)
        == expected
    )
