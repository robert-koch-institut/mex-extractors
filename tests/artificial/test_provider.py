from re import Pattern
from typing import Annotated, Any
from unittest.mock import Mock

import pytest
from faker import Faker
from pydantic import BaseModel, Field
from pydantic.fields import FieldInfo

from mex.common.models import ExtractedContactPoint, ExtractedPrimarySource
from mex.common.testing import Joker
from mex.common.types import (
    APIType,
    Email,
    Identifier,
    Link,
    MergedPrimarySourceIdentifier,
    TemporalEntity,
    TemporalEntityPrecision,
    Text,
    TextLanguage,
)


class DummyModel(BaseModel):
    no_min: list[str] = []
    has_min: list[bytes] = Field([], min_length=2)
    has_max: list[bytes] = Field([], max_length=5)
    is_required: int
    is_optional: bool | None = None
    is_union: float | list[float]
    is_inner_union: list[float | int] = []
    is_union_with_pattern: (
        Annotated[
            str,
            Field(
                pattern=r"^https://www\.wikidata\.org/entity/[PQ0-9]{2,64}$",
            ),
        ]
        | None
    ) = None
    is_nested_pattern: list[
        Annotated[
            str,
            Field(
                pattern=r"^https://gepris\.dfg\.de/gepris/institution/[0-9]{1,64}$",
            ),
        ]
    ] = []


def test_builder_provider_min_max_for_field(faker: Faker) -> None:
    min_max = {
        name: faker.min_max_for_field(field)
        for name, field in DummyModel.model_fields.items()
    }
    assert min_max == {
        "has_max": (0, 5),
        "has_min": (2, 5),
        "is_inner_union": (0, 1),
        "is_nested_pattern": (0, 2),
        "is_optional": (0, 1),
        "is_required": (1, 1),
        "is_union": (1, 1),
        "is_union_with_pattern": (0, 1),
        "no_min": (0, 3),
    }


def test_builder_provider_inner_type_and_pattern(faker: Faker) -> None:
    inner_types = {
        name: faker.inner_type_and_pattern(field)
        for name, field in DummyModel.model_fields.items()
    }
    assert inner_types == {
        "has_min": (bytes, None),
        "has_max": (bytes, None),
        "is_inner_union": (int, None),
        "is_nested_pattern": (
            str,
            "^https://gepris\\.dfg\\.de/gepris/institution/[0-9]{1,64}$",
        ),
        "is_optional": (bool, None),
        "is_required": (int, None),
        "is_union": (float, None),
        "is_union_with_pattern": (
            str,
            "^https://www\\.wikidata\\.org/entity/[PQ0-9]{2,64}$",
        ),
        "no_min": (str, None),
    }


@pytest.mark.parametrize(
    ("annotation", "expected"),
    [
        (
            Link,
            [
                Link(language=None, title="Larsen", url="http://sanchez-nguyen.com/"),
            ],
        ),
        (Email, ["cortezraymond@example.net"]),
        (
            Text,
            [
                Text(
                    value="Ability management test during foot that course nothing.",
                    language=TextLanguage.EN,
                )
            ],
        ),
        (TemporalEntity, [TemporalEntity("1998-07-11T09:38:23Z")]),
        (APIType, [APIType["REST"]]),
        (
            Annotated[Pattern, Field(pattern=r"^https://ror\.org/[a-z0-9]{9}$")],
            ["https://ror.org/160975351"],
        ),
        (
            Annotated[
                str, Field(pattern=(r"^http://id\.nlm\.nih\.gov/mesh/[A-Z0-9]{2,64}$"))
            ],
            ["http://id.nlm.nih.gov/mesh/D000006"],
        ),
        (
            list[
                Annotated[
                    str,
                    Field(
                        pattern=r"^https://gepris\.dfg\.de/gepris/institution/[0-9]{1,64}$",
                    ),
                ]
            ],
            ["https://gepris.dfg.de/gepris/institution/0975351"],
        ),
        (str, ["serve"]),
    ],
)
def test_builder_provider_field_value(
    faker: Faker, annotation: Any, expected: Any
) -> None:
    field = FieldInfo.from_annotation(annotation)
    identity = Mock(stableTargetId="baaiaaaaaaaboi")

    assert faker.field_value(field, identity) == expected


def test_builder_provider_field_value_reference(faker: Faker) -> None:
    field = FieldInfo.from_annotation(MergedPrimarySourceIdentifier)
    identity = Mock(stableTargetId="baaiaaaaaaaboi")
    reference = faker.field_value(field, identity)

    assert set(reference) < {
        i.stableTargetId for i in faker.identities(ExtractedPrimarySource)
    }


def test_builder_provider_field_value_error(faker: Faker) -> None:
    field = FieldInfo.from_annotation(object)
    identity = Mock(stableTargetId="baaiaaaaaaaboi")

    with pytest.raises(RuntimeError, match="Cannot create fake data"):
        faker.field_value(field, identity)


def test_builder_provider_extracted_items(faker: Faker) -> None:
    models = faker.extracted_items(ExtractedContactPoint)
    assert models[0].model_dump(exclude_defaults=True) == {
        "email": [
            "salazarmaria@example.com",
            "jessicapadilla@example.org",
        ],
        "hadPrimarySource": Joker(),
        "identifier": Joker(),
        "identifierInPrimarySource": "ContactPoint-4181830114",
        "stableTargetId": Joker(),
    }


def test_identity_provider_identities(faker: Faker) -> None:
    primary_sources = faker.identities(ExtractedPrimarySource)
    assert len(primary_sources) == 2
    assert primary_sources[0].model_dump() == {
        "hadPrimarySource": MergedPrimarySourceIdentifier("00000000000000"),
        "identifier": Joker(),
        "identifierInPrimarySource": "PrimarySource-2516530558",
        "stableTargetId": Joker(),
    }


def test_identity_provider_reference(faker: Faker) -> None:
    identities = list(faker.identities(ExtractedPrimarySource))

    for identity in identities:
        reference = faker.reference(MergedPrimarySourceIdentifier, identity)
        assert reference != identity.stableTargetId
        assert reference in [i.stableTargetId for i in identities]

    assert faker.reference([], Identifier("00000000000000")) is None


def test_link_provider(faker: Faker) -> None:
    assert faker.link() == Link(language=None, title=None, url="http://www.larsen.com/")


def test_temporal_entity_provider(faker: Faker) -> None:
    assert faker.temporal_entity([TemporalEntityPrecision.DAY]) == TemporalEntity(
        "2022-09-28"
    )


def test_text_provider_string(faker: Faker) -> None:
    assert faker.text_string() == "whole address better"


def test_text_provider_text(faker: Faker) -> None:
    assert faker.text_object() == Text(
        value="Rest human station property. Partner stock four. "
        "Region as true develop sound central.",
        language=TextLanguage.EN,
    )


def test_pattern_provider(faker: Faker) -> None:
    pattern = faker.pattern(r"^https://ror\.org/[a-z0-9]{9}$")
    assert pattern == "https://ror.org/801609753"

    pattern = faker.pattern(
        "^(((http)|(https))://(dx.)?doi.org/)(10.\\d{4,9}/[-._;()/:A-Z0-9]+)$"
    )
    assert pattern == "https://dx.doi.org/10.8242/1948924"

    pattern = faker.pattern(r"^http://id\.nlm\.nih\.gov/mesh/[A-Z0-9]{2,64}$")
    assert pattern == "http://id.nlm.nih.gov/mesh/D000007"
