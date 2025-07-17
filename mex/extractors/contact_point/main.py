from dagster import asset

from mex.common.models import ExtractedContactPoint, ExtractedPrimarySource
from mex.common.types import Email
from mex.extractors.sinks import load

MEX_EMAIL = "mex@rki.de"


@asset(group_name="contact_point")
def extracted_contact_point_mex(
    extracted_primary_source_mex: ExtractedPrimarySource,
) -> None:
    """Load and return blueant primary source."""
    extracted_contact_point = ExtractedContactPoint(
        identifierInPrimarySource=MEX_EMAIL,
        hadPrimarySource=extracted_primary_source_mex.stableTargetId,
        email=[Email(MEX_EMAIL)],
    )
    load([extracted_contact_point])
