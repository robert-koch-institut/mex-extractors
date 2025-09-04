from dagster import asset

from mex.common.cli import entrypoint
from mex.common.models import ExtractedContactPoint, ExtractedPrimarySource
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.settings import Settings
from mex.extractors.sinks import load


@asset(group_name="contact_point")
def extracted_contact_point_mex(
    extracted_primary_source_mex: ExtractedPrimarySource,
) -> list[ExtractedContactPoint]:
    """Load and return default contact points."""
    settings = Settings.get()
    extracted_contact_points = [
        ExtractedContactPoint(
            identifierInPrimarySource=str(settings.contact_point.mex_email),
            hadPrimarySource=extracted_primary_source_mex.stableTargetId,
            email=[settings.contact_point.mex_email],
        )
    ]
    load(extracted_contact_points)
    return extracted_contact_points


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the contact-point extractor job in-process."""
    run_job_in_process("contact_point")
