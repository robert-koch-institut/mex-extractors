import tempfile
from pathlib import Path
from unittest.mock import Mock, call, patch

from mex.extractors.system.main import (
    system_clean_up_dagster_files,
    system_clean_up_dagster_runs,
    system_fetch_old_dagster_run_ids,
)
from tests.system.conftest import create_mock_dagster_run_records


def test_system_fetch_old_dagster_run_ids() -> None:
    # Simulate pagination behavior
    batch_1 = [create_mock_dagster_run_records(f"run_{i}") for i in range(100)]
    batch_2 = [create_mock_dagster_run_records(f"run_{i}") for i in range(100, 150)]
    batch_3: list[Mock] = []  # Empty batch to signal end

    # Mock Dagster instance
    mock_instance = Mock()
    mock_instance.get_run_records.side_effect = [batch_1, batch_2, batch_3]

    with patch("dagster.DagsterInstance.get", return_value=mock_instance):
        result = system_fetch_old_dagster_run_ids()

    # Assertions
    assert len(result) == 150
    assert result[0] == "run_0"
    assert result[-1] == "run_149"

    # 3 calls due to pagination
    assert mock_instance.get_run_records.call_count == 3

    calls = mock_instance.get_run_records.call_args_list
    # Verify cursor was used correctly in subsequent calls
    assert calls[0].kwargs["cursor"] is None
    assert calls[1].kwargs["cursor"] == "run_99"  # last ID from first batch
    assert calls[2].kwargs["cursor"] == "run_149"  # last ID from second batch
    # Assert that a cutoff date filter was passed for all batches
    assert all(c.kwargs["filters"].created_before for c in calls)


def test_system_clean_up_dagster_files() -> None:
    """Test that the function deleted files correctly."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        old_ids = ["run_1", "run_2", "run_3"]
        for run_id in old_ids:
            run_dir = temp_path / run_id
            run_dir.mkdir()  # Create dummy directories
            (run_dir / "dummy_file.txt").touch()  # create files in that directories

        keep_dir = temp_path / "keep_directory"  # directory should NOT be deleted
        keep_dir.mkdir()
        (keep_dir / "keep_file.txt").touch()

        mock_instance = Mock()
        mock_instance.storage_directory.return_value = str(temp_path)

        with patch("dagster.DagsterInstance.get", return_value=mock_instance):
            deleted_ids = system_clean_up_dagster_files(old_ids)

        assert deleted_ids == old_ids

        for run_id in old_ids:
            assert not (temp_path / run_id).exists()  # correct directories were deleted
        assert (
            temp_path / "keep_directory"
        ).exists()  # unrelated directory still there
        assert (temp_path / "keep_directory" / "keep_file.txt").exists()


def test_system_clean_up_dagster_files_with_error_handling() -> None:
    """Test that the function handles errors correctly."""
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        old_ids = ["run_1_missing", "run_2"]
        (temp_path / "run_2").mkdir()

        mock_instance = Mock()
        mock_instance.storage_directory.return_value = str(temp_path)

        with (
            patch(
                "mex.extractors.system.main.DagsterInstance.get",
                return_value=mock_instance,
            ),
            patch("mex.extractors.system.main.logger.warning") as mock_logger,
        ):
            deleted_ids = system_clean_up_dagster_files(old_ids)

            assert deleted_ids == ["run_2"]
            assert not (temp_path / "run_2").exists()
            mock_logger.assert_called_once_with(
                "File not found for: %s", "run_1_missing"
            )


def test_system_clean_up_dagster_files_with_empty_list() -> None:
    """Test that function handles empty input correctly."""
    mock_instance = Mock()
    mock_instance.storage_directory.return_value = "/some/path"

    with patch("dagster.DagsterInstance.get", return_value=mock_instance):
        deleted_ids = system_clean_up_dagster_files([])

    assert deleted_ids == []  # no deleted files logged
    mock_instance.storage_directory.assert_called_once()  # directory was still called


def test_system_clean_up_dagster_runs() -> None:
    """Test that runs are deleted correctly."""
    mock_instance = Mock()
    mock_instance.delete_run = Mock()

    run_ids = ["run_1", "run_2", "run_3"]

    with patch("dagster.DagsterInstance.get", return_value=mock_instance):
        deleted_runs = system_clean_up_dagster_runs(run_ids)

    assert mock_instance.delete_run.call_count == 3

    expected_calls = [call("run_1"), call("run_2"), call("run_3")]
    mock_instance.delete_run.assert_has_calls(expected_calls, any_order=False)
    assert deleted_runs == run_ids


def test_system_clean_up_dagster_runs_with_empty_list() -> None:
    """Test function handles empty input correctly."""
    mock_instance = Mock()

    with patch("dagster.DagsterInstance.get", return_value=mock_instance):
        deleted_runs = system_clean_up_dagster_runs([])

    mock_instance.delete_run.assert_not_called()  # delete_run was never called
    assert deleted_runs == []
