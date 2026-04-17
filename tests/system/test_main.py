import subprocess
import tempfile
from pathlib import Path
from typing import cast
from unittest.mock import Mock, call, patch

import pytest
from dagster import AssetKey

from mex.extractors.system.main import (
    _delete_asset_metadata,
    system_clean_up_dagster_files,
    system_clean_up_dagster_runs,
    system_clean_up_obsolete_assets,
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


def test_system_clean_up_obsolete_assets() -> None:
    """Test that obsolete assets are deleted and protected assets are NOT deleted."""
    current_assets = {
        AssetKey(["asset_1"]),
        AssetKey(["asset_2"]),  # not yet run and therefore not known to dagster
    }

    historical_assets = {
        AssetKey(["asset_1"]),  # still active
        AssetKey(["protected_asset"]),  # protected
        AssetKey(["obsolete_asset"]),  # outdated -> the only one to delete
    }

    mock_definitions = Mock()
    mock_asset_graph = Mock()
    mock_asset_graph.get_all_asset_keys.return_value = current_assets
    mock_definitions.resolve_asset_graph.return_value = mock_asset_graph

    mock_instance = Mock()
    mock_instance.all_asset_keys.return_value = historical_assets

    with (
        patch(
            "mex.extractors.system.main.load_job_definitions",
            return_value=mock_definitions,
        ),
        patch(
            "mex.extractors.system.main.DagsterInstance.get",
            return_value=mock_instance,
        ),
        patch("mex.extractors.system.main._delete_asset_metadata") as mock_delete,
    ):
        system_clean_up_obsolete_assets()

    assert mock_delete.call_count == 1
    assert mock_delete.call_args[0][0] == AssetKey(["obsolete_asset"])


def test_system_clean_up_obsolete_assets_no_obsolete() -> None:
    """Test when there are no obsolete assets."""
    current_assets = {
        AssetKey(["asset_1"]),
        AssetKey(["asset_2"]),
    }

    mock_definitions = Mock()
    mock_asset_graph = Mock()
    mock_asset_graph.get_all_asset_keys.return_value = current_assets
    mock_definitions.resolve_asset_graph.return_value = mock_asset_graph

    mock_instance = Mock()
    mock_instance.all_asset_keys.return_value = current_assets  # historical == current

    with (
        patch(
            "mex.extractors.system.main.load_job_definitions",
            return_value=mock_definitions,
        ),
        patch(
            "mex.extractors.system.main.DagsterInstance.get",
            return_value=mock_instance,
        ),
        patch("mex.extractors.system.main._delete_asset_metadata") as mock_delete,
    ):
        system_clean_up_obsolete_assets()

    # No deletions should occur
    mock_delete.assert_not_called()


def test_delete_asset_metadata_success() -> None:
    """Test successful asset metadata deletion via subprocess."""
    asset_key = AssetKey(["test_asset"])

    with patch("mex.extractors.system.main.subprocess.run") as mock_run:
        _delete_asset_metadata(asset_key)

    mock_run.assert_called_once()
    call_args = mock_run.call_args
    assert call_args[0][0] == [
        "dagster",
        "asset",
        "wipe",
        '["test_asset"]',
        "--noprompt",
    ]


def test_delete_asset_metadata_failure() -> None:
    """Test error handling when asset deletion fails."""
    asset_key = AssetKey(["failing_asset"])

    # Create a CalledProcessError
    error = subprocess.CalledProcessError(
        returncode=1,
        cmd=["dagster", "asset", "wipe", '["doesnt_matter"]', "--noprompt"],
        stderr="stderr output",
    )

    with patch("mex.extractors.system.main.subprocess.run") as mock_run:
        mock_run.side_effect = error
        with (
            patch("mex.extractors.system.main.logger.exception") as mock_logger,
            pytest.raises(RuntimeError, match="Could not wipe Dagster metadata"),
        ):
            _delete_asset_metadata(asset_key)

        mock_logger.assert_called_once()
        # Verify the exception was logged with the correct asset key
        call_args = mock_logger.call_args
        assert "failing_asset" in str(call_args)
