from dagster import asset

from mex.common.cli import entrypoint
from mex.common.models import ExtractedContactPoint
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.settings import Settings
from mex.extractors.sinks import load


@asset(group_name="contact_point")
def contact_point_extracted_contact_points() -> list[ExtractedContactPoint]:
    """Load and return default contact points."""
    settings = Settings.get()
    extracted_contact_points = [
        ExtractedContactPoint(
            identifierInPrimarySource=str(settings.contact_point.mex_email),
            hadPrimarySource=get_extracted_primary_source_id_by_name("mex"),
            email=[settings.contact_point.mex_email],
        )
    ]
    load(extracted_contact_points)
    return extracted_contact_points


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the contact-point extractor job in-process."""
    run_job_in_process("contact_point")
