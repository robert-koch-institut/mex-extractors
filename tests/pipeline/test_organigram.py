from typing import TYPE_CHECKING, cast

from mex.common.models.organization import ExtractedOrganization
from mex.common.types import (
    MergedOrganizationIdentifier,
)
from mex.extractors.pipeline.organigram import (
    extracted_organizational_units,
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
        ("parent-unit", [MergedOrganizationIdentifier("fxIeF3TWocUZoMGmBftJ6x")]),
        ("fg99", [MergedOrganizationIdentifier("fxIeF3TWocUZoMGmBftJ6x")]),
        ("child-unit", [MergedOrganizationIdentifier("fxIeF3TWocUZoMGmBftJ6x")]),
    ]
