from mex.common.models import ExtractedPrimarySource
from mex.common.testing import Joker
from mex.common.types import Identifier, TemporalEntity, TextLanguage
from mex.extractors.rdmo.models.person import RDMOPerson
from mex.extractors.rdmo.models.source import RDMOSource
from mex.extractors.rdmo.transform import transform_rdmo_sources_to_extracted_activities


def test_transform_rdmo_sources_to_extracted_activities(
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
) -> None:
    rdmo_owner = RDMOPerson(id="0", username="a", email="ausgedachta@rki.de")
    rdmo_source = RDMOSource(
        description="ABC",
        id=123,
        title="abc",
        owners=[rdmo_owner],
        question_answer_pairs={
            "/domain/project/title": "A, B and C",
            "/domain/project/type": "https://mex.rki.de/item/activity-type-3",
            "/domain/project/schedule/project_start": "2004",
            "/domain/project/schedule/project_end": "2006-05",
            "/domain/project/coordination/contact/name": str(
                Identifier.generate(seed=42)
            ),
            "/domain/project/coordination/unit": str(Identifier.generate(seed=24)),
        },
    )
    mex_sources = list(
        transform_rdmo_sources_to_extracted_activities(
            [rdmo_source], extracted_primary_sources["rdmo"]
        )
    )

    assert len(mex_sources) == 1
    assert mex_sources[0].model_dump(exclude_none=True, exclude_defaults=True) == {
        "abstract": [{"value": "ABC"}],
        "activityType": ["https://mex.rki.de/item/activity-type-3"],
        "contact": [Identifier.generate(seed=42)],
        "end": [TemporalEntity("2006-05")],
        "hadPrimarySource": extracted_primary_sources["rdmo"].stableTargetId,
        "identifierInPrimarySource": "123",
        "identifier": Joker(),
        "responsibleUnit": [Identifier.generate(seed=24)],
        "stableTargetId": Joker(),
        "start": [TemporalEntity("2004")],
        "theme": ["https://mex.rki.de/item/theme-1"],
        "title": [{"language": TextLanguage.EN, "value": "A, B and C"}],
    }
