from typing import TYPE_CHECKING, cast
from unittest.mock import MagicMock, call

import pytest

from mex.common.types import MergedContactPointIdentifier
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.publisher.main import (
    fallback_contact_identifiers,
    publishable_contact_points_and_units,
    publishable_items,
    publishable_items_without_actors,
    publishable_persons,
)
from mex.extractors.sinks.s3 import S3Sink

if TYPE_CHECKING:
    from mex.common.models import AnyMergedModel, ItemsContainer


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
def test_publishable_items_without_actors(mocked_backend: MagicMock) -> None:
    container = cast(
        "ItemsContainer[AnyMergedModel]", publishable_items_without_actors()
    )
    assert len(container.items) == 1
    mocked_backend.fetch_extracted_items.assert_not_called()
    assert mocked_backend.fetch_merged_items.call_args_list == [
        call(
            query_string=None,
            entity_type=[
                "MergedAccessPlatform",
                "MergedActivity",
                "MergedBibliographicResource",
                "MergedDistribution",
                "MergedOrganization",
                "MergedResource",
                "MergedVariable",
                "MergedVariableGroup",
            ],
            had_primary_source=None,
            skip=0,
            limit=1,
        ),
        call(
            query_string=None,
            entity_type=[
                "MergedAccessPlatform",
                "MergedActivity",
                "MergedBibliographicResource",
                "MergedDistribution",
                "MergedOrganization",
                "MergedResource",
                "MergedVariable",
                "MergedVariableGroup",
            ],
            had_primary_source=None,
            skip=0,
            limit=100,
        ),
    ]


@pytest.mark.usefixtures("mocked_backend", "mocked_boto")
def test_publishable_persons(mocked_backend: MagicMock) -> None:
    container = cast("ItemsContainer[AnyMergedModel]", publishable_persons())
    assert len(container.items) == 1
    assert mocked_backend.fetch_extracted_items.call_args_list == [
        call(None, None, ["ExtractedPrimarySource"], 0, 100)
    ]
    assert mocked_backend.fetch_merged_items.call_args_list == [
        call(
            query_string=None,
            entity_type=["MergedPerson"],
            had_primary_source=["hSHhxBonhhI8TpMqFqSFKl"],
            skip=0,
            limit=1,
        ),
        call(
            query_string=None,
            entity_type=["MergedPerson"],
            had_primary_source=["hSHhxBonhhI8TpMqFqSFKl"],
            skip=0,
            limit=100,
        ),
    ]


def test_publishable_contact_points_and_units(mocked_backend: MagicMock) -> None:
    container = cast(
        "ItemsContainer[AnyMergedModel]", publishable_contact_points_and_units()
    )
    assert len(container.items) == 2
    mocked_backend.fetch_extracted_items.assert_not_called()
    assert mocked_backend.fetch_merged_items.call_args_list == [
        call(
            query_string=None,
            entity_type=["MergedContactPoint", "MergedOrganizationalUnit"],
            had_primary_source=None,
            skip=0,
            limit=1,
        ),
        call(
            query_string=None,
            entity_type=["MergedContactPoint", "MergedOrganizationalUnit"],
            had_primary_source=None,
            skip=0,
            limit=100,
        ),
    ]


@pytest.mark.usefixtures("mocked_backend")
def test_fallback_contact_identifiers() -> None:
    identifiers = cast(
        "list[MergedContactPointIdentifier]", fallback_contact_identifiers()
    )
    assert identifiers == [MergedContactPointIdentifier("fakeFakeContact")]


@pytest.mark.usefixtures("mocked_backend")
def test_publishable_items() -> None:
    container = cast(
        "ItemsContainer[AnyMergedModel]",
        publishable_items(
            publishable_items_without_actors(),
            publishable_persons(),
            publishable_contact_points_and_units(),
            fallback_contact_identifiers(),
        ),
    )
    assert len(container.items) == 4
