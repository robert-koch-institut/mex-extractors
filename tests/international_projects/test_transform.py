from pytz import timezone

from mex.common.models import ActivityMapping
from mex.common.testing import Joker
from mex.common.types import (
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
    TextLanguage,
    YearMonthDay,
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


def test_transform_international_projects_source_to_mex_source(
    international_projects_mapping_activity: ActivityMapping,
) -> None:
    organization_id = MergedOrganizationIdentifier.generate(seed=44)
    funding_source_stable_target_ids_by_synonym = {"Test-Institute": organization_id}
    partner_organizations_stable_target_ids_by_synonym = {"WHO": organization_id}
    person_id = MergedPersonIdentifier.generate(seed=30)
    person_stable_target_ids_by_query_string = {"Dr Frieda Ficticious": [person_id]}

    international_projects_sources = list(extract_international_projects_sources())

    extracted_activities = list(
        transform_international_projects_sources_to_extracted_activities(
            international_projects_sources,
            international_projects_mapping_activity,
            person_stable_target_ids_by_query_string,
            funding_source_stable_target_ids_by_synonym,
            partner_organizations_stable_target_ids_by_synonym,
        )
    )

    expected = {
        "identifier": Joker(),
        "hadPrimarySource": get_extracted_primary_source_id_by_name(
            "international-projects"
        ),
        "identifierInPrimarySource": "0000-1000",
        "stableTargetId": Joker(),
        "activityType": ["https://mex.rki.de/item/activity-type-1"],
        "alternativeTitle": [{"value": "testAAbr"}],
        "contact": ["bFQoRhcVH5DHUU", "cjna2jitPngp6yIV63cdi9"],
        "end": [YearMonthDay(2021, 12, 31, tzinfo=timezone("UTC"))],
        "externalAssociate": [organization_id],
        "funderOrCommissioner": [organization_id],
        "involvedPerson": [person_id],
        "involvedUnit": ["cjna2jitPngp6yIV63cdi9"],
        "responsibleUnit": ["cjna2jitPngp6yIV63cdi9"],
        "shortName": [{"value": "testAAbr"}],
        "start": [YearMonthDay(2021, 7, 27, tzinfo=timezone("UTC"))],
        "theme": ["https://mex.rki.de/item/theme-37"],
        "title": [
            {"value": "This is a test project full title", "language": TextLanguage.EN}
        ],
    }

    assert (
        extracted_activities[0].model_dump(exclude_none=True, exclude_defaults=True)
        == expected
    )
