from mex.extractors.pipeline import run_job_in_process


def test_job() -> None:
    assert run_job_in_process("contact_point")
