from mex.extractors.pipeline import run_job_in_process


def test_job() -> None:
    result = run_job_in_process("organigram")
    assert result.success
