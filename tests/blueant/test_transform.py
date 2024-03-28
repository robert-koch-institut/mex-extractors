from collections.abc import Hashable
from typing import cast

from mex.blueant.models.source import BlueAntSource
from mex.blueant.transform import transform_blueant_sources_to_extracted_activities
from mex.common.models import ExtractedAccessPlatform
from mex.common.testing import Joker
from mex.common.types import Identifier, TemporalEntity


def test_transform_blueant_sources_to_extracted_activities(
    blueant_source: BlueAntSource,
    blueant_source_without_leader: BlueAntSource,
    extracted_primary_sources: dict[str, ExtractedAccessPlatform],
) -> None:
    stable_target_ids_by_employee_id = {
        "person-567": [
            Identifier.generate(seed=99),
        ],
        None: [],
    }
    unit_stable_target_ids_by_synonym = {
        "C1": Identifier.generate(seed=555),
        "C1 Child Department": Identifier.generate(seed=555),
        "XY": Identifier.generate(seed=999),
    }

    mex_sources = list(
        transform_blueant_sources_to_extracted_activities(
            [blueant_source, blueant_source_without_leader],
            extracted_primary_sources["blueant"],
            cast(dict[Hashable, list[Identifier]], stable_target_ids_by_employee_id),
            unit_stable_target_ids_by_synonym,
        )
    )
    assert len(mex_sources) == 2
    assert mex_sources[0].model_dump(exclude_none=True, exclude_defaults=True) == {
        "contact": [Identifier.generate(seed=99)],
        "end": [TemporalEntity("2019-12-31")],
        "responsibleUnit": [Identifier.generate(seed=555)],
        "identifier": Joker(),
        "identifierInPrimarySource": "00123",
        "involvedPerson": [Identifier("bFQoRhcVH5DHV1")],
        "title": [{"value": "3_Prototype Space Rocket", "language": "en"}],
        "activityType": ["https://mex.rki.de/item/activity-type-3"],
        "start": [TemporalEntity("2019-01-07")],
        "hadPrimarySource": extracted_primary_sources["blueant"].stableTargetId,
        "stableTargetId": Joker(),
        "theme": ["https://mex.rki.de/item/theme-1"],
    }
    assert mex_sources[1].model_dump(exclude_none=True, exclude_defaults=True) == {
        "contact": [Identifier("bFQoRhcVH5DH3n")],
        "end": [TemporalEntity("2010-10-11")],
        "responsibleUnit": [Identifier("bFQoRhcVH5DH3n")],
        "identifier": Joker(),
        "identifierInPrimarySource": "00255",
        "involvedPerson": [Identifier("bFQoRhcVH5DH3n")],
        "title": [{"value": "2_Prototype Moon Lander", "language": "en"}],
        "activityType": ["https://mex.rki.de/item/activity-type-6"],
        "start": [TemporalEntity("2018-08-09")],
        "hadPrimarySource": extracted_primary_sources["blueant"].stableTargetId,
        "stableTargetId": Joker(),
        "theme": ["https://mex.rki.de/item/theme-1"],
    }
