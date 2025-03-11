from mex.extractors.seq_repo.filter import filter_sources_on_latest_sequencing_date
from mex.extractors.seq_repo.model import SeqRepoSource


def test_filter_sources_on_latest_sequencing_date(
    seq_repo_sources: list[SeqRepoSource],
) -> None:
    seq_repo_sources_filtered = filter_sources_on_latest_sequencing_date(
        seq_repo_sources
    )

    expected = {
        "test-sample-id.TEST": SeqRepoSource(
            project_coordinators=["max", "mustermann", "max"],
            customer_org_unit_id="FG99",
            sequencing_date="2023-08-07",
            lims_sample_id="test-sample-id",
            sequencing_platform="TEST",
            species="Severe acute respiratory syndrome coronavirus 2",
            project_name="FG99-ABC-123",
            customer_sample_name="test-customer-name-1",
            project_id="TEST-ID",
        )
    }

    assert len(seq_repo_sources) == 2
    assert len(seq_repo_sources_filtered) == 1
    assert seq_repo_sources_filtered == expected
