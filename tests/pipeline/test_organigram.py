from typing import cast

import pytest

from mex.common.models import ExtractedOrganizationalUnit, ExtractedPrimarySource
from mex.common.models.organization import ExtractedOrganization
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPrimarySourceIdentifier,
)
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
    extracted_organization_rki: ExtractedOrganization,
) -> None:
    units = cast(
        "list[ExtractedOrganizationalUnit]",
        extracted_organizational_units(
            extracted_primary_source_organigram, extracted_organization_rki
        ),
    )
    assert [(u.identifierInPrimarySource, u.unitOf) for u in units] == [
        ("child-unit", [MergedOrganizationIdentifier("fxIeF3TWocUZoMGmBftJ6x")]),
        ("parent-unit", [MergedOrganizationIdentifier("fxIeF3TWocUZoMGmBftJ6x")]),
        ("fg99", [MergedOrganizationIdentifier("fxIeF3TWocUZoMGmBftJ6x")]),
    ]


def test_unit_stable_target_ids_by_synonym(
    extracted_primary_source_organigram: ExtractedPrimarySource,
    extracted_organization_rki: ExtractedOrganization,
) -> None:
    units_by_synonym = cast(
        "dict[str, MergedOrganizationalUnitIdentifier]",
        unit_stable_target_ids_by_synonym(
            extracted_organizational_units(
                extracted_primary_source_organigram, extracted_organization_rki
            )
        ),
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
