from mex.common.models import ActivityMapping
from mex.common.testing import Joker
from mex.common.types import (
    Identifier,
    MergedOrganizationalUnitIdentifier,
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


def test_transform_blueant_sources_to_extracted_activities(
    blueant_source: BlueAntSource,
    blueant_source_without_leader: BlueAntSource,
    blueant_activity: ActivityMapping,
) -> None:
    stable_target_ids_by_employee_id = {
        "42": [
            MergedPersonIdentifier.generate(seed=99),
        ],
    }
    unit_stable_target_ids_by_synonym = {
        "C1": [MergedOrganizationalUnitIdentifier.generate(seed=555)],
        "C1 Child Department": [MergedOrganizationalUnitIdentifier.generate(seed=555)],
        "XY": [MergedOrganizationalUnitIdentifier.generate(seed=999)],
    }
    mex_sources = list(
        transform_blueant_sources_to_extracted_activities(
            [blueant_source, blueant_source_without_leader],
            stable_target_ids_by_employee_id,
            unit_stable_target_ids_by_synonym,
            blueant_activity,
            {"Robert Koch-Institut": MergedOrganizationIdentifier.generate(seed=42)},
        )
    )
    assert len(mex_sources) == 2
    assert mex_sources[0].model_dump(exclude_none=True, exclude_defaults=True) == {
        "contact": [str(Identifier.generate(seed=99))],
        "responsibleUnit": [str(Identifier.generate(seed=555))],
        "funderOrCommissioner": [str(Identifier.generate(seed=42))],
        "identifier": Joker(),
        "identifierInPrimarySource": "00123",
        "involvedPerson": ["bFQoRhcVH5DHV1"],
        "title": [{"value": "Prototype Space Rocket", "language": TextLanguage.EN}],
        "start": ["2019-01-07"],
        "hadPrimarySource": str(get_extracted_primary_source_id_by_name("blueant")),
        "stableTargetId": Joker(),
    }
    assert mex_sources[1].model_dump(exclude_none=True, exclude_defaults=True) == {
        "contact": ["bFQoRhcVH5DHV1"],
        "funderOrCommissioner": [str(Identifier.generate(seed=42))],
        "responsibleUnit": ["bFQoRhcVH5DH3n"],
        "identifier": Joker(),
        "identifierInPrimarySource": "00255",
        "title": [{"value": "Prototype Moon Lander", "language": TextLanguage.EN}],
        "start": ["2018-08-09"],
        "hadPrimarySource": str(get_extracted_primary_source_id_by_name("blueant")),
        "stableTargetId": Joker(),
    }
