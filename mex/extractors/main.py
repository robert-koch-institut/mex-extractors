from mex.common.cli import entrypoint
from mex.extractors.pipeline.base import run_job_in_process
from mex.extractors.settings import ExtractorSettings


@entrypoint(ExtractorSettings)
def run() -> None:  # pragma: no cover
    """Run all extractor jobs in-process."""
    run_job_in_process("all_extractors")
