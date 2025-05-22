from typing import TYPE_CHECKING

from dagster import asset

from mex.artificial.helpers import generate_artificial_extracted_items
from mex.artificial.main import DEFAULT_LOCALE
from mex.common.cli import entrypoint
from mex.common.models import EXTRACTED_MODEL_CLASSES_BY_NAME
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.settings import Settings
from mex.extractors.sinks import load

if TYPE_CHECKING:
    from collections.abc import Sequence


@asset(group_name="artificial")
def artificial_data() -> None:
    """Load the artificial data models to the sinks."""
    models: Sequence[str] = list(EXTRACTED_MODEL_CLASSES_BY_NAME.keys())
    artificial_data = generate_artificial_extracted_items(
        locale=DEFAULT_LOCALE,
        seed=42,
        count=len(models) * 25,
        chattiness=16,
        stem_types=models,
    )
    load(artificial_data)


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the artificial data job in-process."""
    run_job_in_process("artificial")
