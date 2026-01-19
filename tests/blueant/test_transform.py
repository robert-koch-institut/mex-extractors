import pytest

from mex.common.models import ActivityMapping
from mex.common.testing import Joker
from mex.common.types import (
    Identifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
    TextLanguage,
)
from mex.extractors.blueant.models.source import BlueAntSource
from mex.extractors.blueant.transform import (
    transform_blueant_sources_to_extracted_activities,
)
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)


@pytest.mark.usefixtures("mocked_wikidata")
def test_transform_blueant_sources_to_extracted_activities(
    blueant_source: BlueAntSource,
    blueant_source_without_leader: BlueAntSource,
    blueant_activity: ActivityMapping,
) -> None:
    stable_target_ids_by_employee_id = {
        "person-567": [
            MergedPersonIdentifier.generate(seed=99),
        ],
    }
    mex_sources = transform_blueant_sources_to_extracted_activities(
        [blueant_source, blueant_source_without_leader],
        stable_target_ids_by_employee_id,
        blueant_activity,
        {"Robert Koch-Institut": MergedOrganizationIdentifier.generate(seed=42)},
    )
    assert len(mex_sources) == 2
    assert mex_sources[0].model_dump(exclude_none=True, exclude_defaults=True) == {
        "contact": [str(Identifier.generate(seed=99))],
        "responsibleUnit": ["6rqNvZSApUHlz8GkkVP48"],
        "funderOrCommissioner": [str(Identifier.generate(seed=42))],
        "identifier": Joker(),
        "identifierInPrimarySource": "00123",
        "involvedPerson": ["bFQoRhcVH5DHV1"],
        "title": [{"value": "Prototype Space Rocket", "language": TextLanguage.EN}],
        "start": ["2019-01-07"],
        "hadPrimarySource": str(get_extracted_primary_source_id_by_name("blueant")),
        "stableTargetId": Joker(),
    }
