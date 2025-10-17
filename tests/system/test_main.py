import tempfile
from pathlib import Path
from typing import cast
from unittest.mock import Mock, call, patch

from mex.extractors.system.main import (
    system_clean_up_dagster_files,
    system_clean_up_dagster_runs,
    system_fetch_old_dagster_run_ids,
)
from tests.system.conftest import create_mock_dagster_run_records


def test_system_fetch_old_dagster_run_ids() -> None:
    # Simulate pagination behavior
    mocked_old_runs = [create_mock_dagster_run_records(f"run_{i}") for i in range(42)]

    # Mock Dagster instance
    mock_instance = Mock()
    mock_instance.get_run_records.side_effect = [mocked_old_runs]

    with patch(
        "mex.extractors.system.main.DagsterInstance.get", return_value=mock_instance
    ):
        result = cast("list[str]", system_fetch_old_dagster_run_ids())

    assert len(result) == 42
    assert result[0] == "run_0"
    assert result[-1] == "run_41"

    calls = mock_instance.get_run_records.call_args_list
    # Assert that a cutoff date filter was passed
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

        with patch(
            "mex.extractors.system.main.DagsterInstance.get", return_value=mock_instance
        ):
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

    with patch(
        "mex.extractors.system.main.DagsterInstance.get", return_value=mock_instance
    ):
        deleted_ids = system_clean_up_dagster_files([])

    assert deleted_ids == []  # no deleted files logged
    mock_instance.storage_directory.assert_called_once()  # directory was still called


def test_system_clean_up_dagster_runs() -> None:
    """Test that runs are deleted correctly."""
    mock_instance = Mock()
    mock_instance.delete_run = Mock()

    run_ids = ["run_1", "run_2", "run_3"]

    with patch(
        "mex.extractors.system.main.DagsterInstance.get", return_value=mock_instance
    ):
        deleted_runs = system_clean_up_dagster_runs(run_ids)

    assert mock_instance.delete_run.call_count == 3

    expected_calls = [call("run_1"), call("run_2"), call("run_3")]
    mock_instance.delete_run.assert_has_calls(expected_calls, any_order=False)
    assert deleted_runs == run_ids


def test_system_clean_up_dagster_runs_with_empty_list() -> None:
    """Test function handles empty input correctly."""
    mock_instance = Mock()

    with patch(
        "mex.extractors.system.main.DagsterInstance.get", return_value=mock_instance
    ):
        deleted_runs = system_clean_up_dagster_runs([])

    mock_instance.delete_run.assert_not_called()  # delete_run was never called
    assert deleted_runs == []
