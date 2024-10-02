from collections.abc import Hashable
from typing import Any, cast

from mex.common.models import ExtractedAccessPlatform
from mex.common.testing import Joker
from mex.common.types import (
    Identifier,
    MergedOrganizationIdentifier,
    TextLanguage,
    YearMonthDay,
)
from mex.extractors.blueant.models.source import BlueAntSource
from mex.extractors.blueant.transform import (
    transform_blueant_sources_to_extracted_activities,
)


def test_transform_blueant_sources_to_extracted_activities(
    blueant_source: BlueAntSource,
    blueant_source_without_leader: BlueAntSource,
    extracted_primary_sources: dict[str, ExtractedAccessPlatform],
    blueant_activity: dict[str, Any],
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
            blueant_activity,
            {"Robert Koch-Institut": MergedOrganizationIdentifier.generate(seed=42)},
        )
    )
    assert len(mex_sources) == 2
    assert mex_sources[0].model_dump(exclude_none=True, exclude_defaults=True) == {
        "contact": [str(Identifier.generate(seed=99))],
        "end": [YearMonthDay("2019-12-31")],
        "responsibleUnit": [str(Identifier.generate(seed=555))],
        "funderOrCommissioner": [str(MergedOrganizationIdentifier.generate(seed=42))],
        "identifier": Joker(),
        "identifierInPrimarySource": "00123",
        "involvedPerson": ["bFQoRhcVH5DHV1"],
        "title": [{"value": "3_Prototype Space Rocket", "language": TextLanguage.EN}],
        "start": [YearMonthDay("2019-01-07")],
        "hadPrimarySource": str(extracted_primary_sources["blueant"].stableTargetId),
        "stableTargetId": Joker(),
    }
    assert mex_sources[1].model_dump(exclude_none=True, exclude_defaults=True) == {
        "contact": ["bFQoRhcVH5DH3n"],
        "end": [YearMonthDay("2010-10-11")],
        "funderOrCommissioner": [str(MergedOrganizationIdentifier.generate(seed=42))],
        "responsibleUnit": ["bFQoRhcVH5DH3n"],
        "identifier": Joker(),
        "identifierInPrimarySource": "00255",
        "involvedPerson": ["bFQoRhcVH5DH3n"],
        "title": [{"value": "2_Prototype Moon Lander", "language": TextLanguage.EN}],
        "start": [YearMonthDay("2018-08-09")],
        "hadPrimarySource": str(extracted_primary_sources["blueant"].stableTargetId),
        "stableTargetId": Joker(),
    }
