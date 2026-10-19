from typing import TYPE_CHECKING, cast

from mex.extractors.pipeline.organigram import (
    extracted_organizational_units,
)

if TYPE_CHECKING:
    from mex.common.models import ExtractedOrganization, ExtractedOrganizationalUnit


def test_extracted_organizational_units(
    extracted_organization_rki: ExtractedOrganization,
) -> None:
    units = cast(
        "list[ExtractedOrganizationalUnit]",
        extracted_organizational_units(extracted_organization_rki),
    )
    assert [(u.identifierInPrimarySource, u.unitOf) for u in units] == [
        ("parent-unit", [extracted_organization_rki.stableTargetId]),
        ("fg99", [extracted_organization_rki.stableTargetId]),
        ("child-unit", [extracted_organization_rki.stableTargetId]),
    ]
