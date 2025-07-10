from typing import cast

import pytest

from mex.common.models import ExtractedPrimarySource
from mex.common.types import ExtractedOrganizationalUnit, MergedPrimarySourceIdentifier
from mex.extractors.pipeline.organigram import (
    extracted_organizational_units,
    unit_stable_target_ids_by_synonym,
)


@pytest.fixture
def extracted_primary_source_organigram() -> ExtractedPrimarySource:
    return ExtractedPrimarySource(
        identifierInPrimarySource="202",
        hadPrimarySource=MergedPrimarySourceIdentifier.generate(seed=20),
        title="organigram",
    )


def test_extracted_organizational_units(
    extracted_primary_source_organigram: ExtractedPrimarySource,
) -> None:
    units = cast(
        "list[ExtractedOrganizationalUnit]",
        extracted_organizational_units(extracted_primary_source_organigram),
    )
    assert [u.identifierInPrimarySource for u in units] == [
        "child-unit",
        "parent-unit",
        "fg99",
    ]


def test_unit_stable_target_ids_by_synonym(
    extracted_primary_source_organigram: ExtractedPrimarySource,
) -> None:
    units_by_synonym = unit_stable_target_ids_by_synonym(
        extracted_organizational_units(extracted_primary_source_organigram)
    )
    assert sorted(units_by_synonym) == [
        "Abteilung",
        "C1",
        "C1 Sub-Unit",
        "C1 Unterabteilung",
        "C1: Sub Unit",
        "CHLD",
        "CHLD Unterabteilung",
        "Department",
        "FG 99",
        "FG99",
        "Fachgebiet 99",
        "Group 99",
        "PARENT Dept.",
        "PRNT",
        "PRNT Abteilung",
        "child-unit",
        "fg99",
        "parent-unit",
    ]
