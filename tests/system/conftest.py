from unittest.mock import Mock


def create_mock_dagster_run_records(run_id: str) -> Mock:
    # Create mock run records
    mock_record = Mock()
    mock_record.dagster_run.run_id = run_id
    return mock_record
