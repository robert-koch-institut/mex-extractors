from typing import cast
from unittest.mock import MagicMock, call

import pytest

from mex.common.exceptions import MExError
from mex.common.models import AnyMergedModel, ItemsContainer, PaginatedItemsContainer
from mex.common.types import MergedContactPointIdentifier
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.publisher.main import (
    mex_contact_point_identifier,
    publishable_contact_points_and_units,
    publishable_items,
    publishable_items_without_contacts,
    publishable_persons,
)
from mex.extractors.sinks.s3 import S3Sink


@pytest.fixture  # needed for hardcoded upload to S3. Remove with MX-1808
def mocked_boto(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    mocked_client = MagicMock()
    monkeypatch.setattr(
        S3Sink, "__init__", lambda self: setattr(self, "client", mocked_client)
    )
    return mocked_client


@pytest.mark.usefixtures("mocked_backend", "mocked_boto")
def test_run() -> None:
    assert run_job_in_process("publisher")


@pytest.mark.usefixtures("mocked_backend", "mocked_boto")
def test_publishable_items_without_contacts(mocked_backend: MagicMock) -> None:
    container = cast(
        "ItemsContainer[AnyMergedModel]", publishable_items_without_contacts()
    )
    assert len(container.items) == 5
    mocked_backend.fetch_extracted_items.assert_not_called()
    assert mocked_backend.fetch_merged_items.call_args_list == [
        call(None, None, None, 0, 1),
        call(
            None,
            [
                "MergedAccessPlatform",
                "MergedActivity",
                "MergedBibliographicResource",
                "MergedDistribution",
                "MergedOrganization",
                "MergedResource",
                "MergedVariable",
                "MergedVariableGroup",
            ],
            None,
            0,
            100,
        ),
    ]


@pytest.mark.usefixtures("mocked_backend", "mocked_boto")
def test_publishable_persons(mocked_backend: MagicMock) -> None:
    container = cast("ItemsContainer[AnyMergedModel]", publishable_persons())
    assert len(container.items) == 5
    assert mocked_backend.fetch_extracted_items.call_args_list == [
        call(None, None, ["ExtractedPrimarySource"], 0, 100)
    ]
    assert mocked_backend.fetch_merged_items.call_args_list == [
        call(None, None, None, 0, 1),
        call(None, ["MergedPerson"], ["hSHhxBonhhI8TpMqFqSFKl"], 0, 100),
    ]


def test_publishable_contact_points_and_units(mocked_backend: MagicMock) -> None:
    container = cast(
        "ItemsContainer[AnyMergedModel]", publishable_contact_points_and_units()
    )
    assert len(container.items) == 5
    mocked_backend.fetch_extracted_items.assert_not_called()
    assert mocked_backend.fetch_merged_items.call_args_list == [
        call(None, None, None, 0, 1),
        call(
            None,
            ["MergedContactPoint", "MergedOrganizationalUnit"],
            None,
            0,
            100,
        ),
    ]


@pytest.mark.usefixtures("mocked_backend")
def test_mex_contact_point_identifier() -> None:
    identifier = cast("MergedContactPointIdentifier", mex_contact_point_identifier())
    assert identifier == MergedContactPointIdentifier("32")


def test_mex_contact_point_identifier_missing(mocked_backend: MagicMock) -> None:
    mocked_backend.fetch_merged_items.return_value = PaginatedItemsContainer[
        AnyMergedModel
    ](total=0, items=[])
    with pytest.raises(MExError, match="Found 0 ContactPoints"):
        mex_contact_point_identifier()


@pytest.mark.usefixtures("mocked_backend")
def test_publishable_items() -> None:
    container = cast("ItemsContainer[AnyMergedModel]", publishable_items())
    assert len(container.items) == 21
