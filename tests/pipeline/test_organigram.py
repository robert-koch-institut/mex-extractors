from typing import TYPE_CHECKING, cast

from mex.common.models.organization import ExtractedOrganization
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
)
from mex.extractors.pipeline.organigram import (
    extracted_organizational_units,
    unit_stable_target_ids_by_synonym,
)

if TYPE_CHECKING:
    from mex.common.models import ExtractedOrganizationalUnit


def test_extracted_organizational_units(
    extracted_organization_rki: ExtractedOrganization,
) -> None:
    units = cast(
        "list[ExtractedOrganizationalUnit]",
        extracted_organizational_units(extracted_organization_rki),
    )
    assert [(u.identifierInPrimarySource, u.unitOf) for u in units] == [
        ("child-unit", [MergedOrganizationIdentifier("fxIeF3TWocUZoMGmBftJ6x")]),
        ("parent-unit", [MergedOrganizationIdentifier("fxIeF3TWocUZoMGmBftJ6x")]),
        ("fg99", [MergedOrganizationIdentifier("fxIeF3TWocUZoMGmBftJ6x")]),
    ]


def test_unit_stable_target_ids_by_synonym(
    extracted_organization_rki: ExtractedOrganization,
) -> None:
    units_by_synonym = cast(
        "dict[str, MergedOrganizationalUnitIdentifier]",
        unit_stable_target_ids_by_synonym(
            extracted_organizational_units(extracted_organization_rki)
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
