from dagster import asset

from mex.artificial.constants import DEFAULT_LOCALE
from mex.artificial.helpers import generate_artificial_extracted_items
from mex.common.cli import entrypoint
from mex.common.models import (
    EXTRACTED_MODEL_CLASSES_BY_NAME,
    AnyExtractedModel,
    ItemsContainer,
)
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.settings import Settings
from mex.extractors.sinks import load


@asset(group_name="artificial")
def artificial_data() -> ItemsContainer[AnyExtractedModel]:
    """Load the artificial data models to the sinks."""
    artificial_data = generate_artificial_extracted_items(
        locale=DEFAULT_LOCALE,
        seed=42,
        chattiness=16,
        stem_types=list(EXTRACTED_MODEL_CLASSES_BY_NAME),
    )
    load(artificial_data)
    return ItemsContainer[AnyExtractedModel](items=artificial_data)


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the artificial data job in-process."""
    run_job_in_process("artificial")
