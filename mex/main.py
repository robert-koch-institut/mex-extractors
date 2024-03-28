from mex.common.cli import entrypoint
from mex.pipeline.base import run_job_in_process
from mex.settings import Settings


@entrypoint(Settings)
def run() -> None:
    """Run all extractor jobs in-process."""
    run_job_in_process("all_assets")
