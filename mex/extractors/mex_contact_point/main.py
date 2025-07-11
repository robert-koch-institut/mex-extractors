from dagster import asset

from mex.common.models import ExtractedContactPoint, ExtractedPrimarySource
from mex.common.types import Email
from mex.extractors.sinks import load


@asset(group_name="mex_contact_point")
def extracted_contact_point_mex(
    extracted_primary_source_mex: ExtractedPrimarySource,
) -> None:
    """Load and return blueant primary source."""
    extracted_contact_point = ExtractedContactPoint(
        identifierInPrimarySource="mex@rki.de",
        hadPrimarySource=extracted_primary_source_mex.stableTargetId,
        email=[Email("mex@rki.de")],
    )
    load([extracted_contact_point])
