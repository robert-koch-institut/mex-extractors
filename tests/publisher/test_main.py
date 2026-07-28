from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock, call

import pytest

from mex.common.models import (
    ExtractedOrganization,
    ItemsContainer,
    MergedBibliographicResource,
)
from mex.common.types import (
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedPersonIdentifier,
)
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.publisher.main import (
    publisher_contact_points_and_units,
    publisher_fallback_contact_identifiers,
    publisher_items,
    publisher_items_without_actors,
    publisher_persons,
    publisher_sink_load,
)
from mex.extractors.settings import ExtractorsSettings
from mex.extractors.sinks.ndjson import NdjsonSink
from mex.extractors.sinks.s3 import S3Sink

if TYPE_CHECKING:
    from mex.extractors.publisher.types import PublisherItemsLike


@pytest.mark.usefixtures(
    "mocked_backend",
    "mocked_s3sink_client",  # needed for hardcoded upload to S3. Remove with MX-1808
)
def test_run() -> None:
    assert run_job_in_process("publisher")


@pytest.mark.usefixtures("mocked_backend")
def test_publisher_items_without_actors(mocked_backend: MagicMock) -> None:
    container = cast("PublisherItemsLike", publisher_items_without_actors())
    assert len(container.items) == 1
    assert isinstance(container.items[0], MergedBibliographicResource)
    mocked_backend.fetch_extracted_items.assert_not_called()
    assert mocked_backend.fetch_all_publishable_merged_items.call_args_list == [
        call(
            publishing_target="invenio",
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
            referenced_identifier=None,
            reference_field=None,
        ),
    ]


@pytest.mark.usefixtures("mocked_backend")
def test_publisher_persons(mocked_backend: MagicMock) -> None:
    container = cast("PublisherItemsLike", publisher_persons())
    assert len(container.items) == 1
    assert mocked_backend.fetch_all_publishable_merged_items.call_args_list == [
        call(
            publishing_target="invenio",
            query_string=None,
            entity_type=["MergedPerson"],
            referenced_identifier=None,
            reference_field=None,
        ),
        call(
            publishing_target="invenio",
            query_string=None,
            entity_type=["MergedConsent"],
            referenced_identifier=None,
            reference_field=None,
        ),
    ]


def test_publisher_contact_points_and_units(mocked_backend: MagicMock) -> None:
    container = cast("PublisherItemsLike", publisher_contact_points_and_units())
    assert len(container.items) == 2
    mocked_backend.fetch_extracted_items.assert_not_called()
    assert mocked_backend.fetch_all_publishable_merged_items.call_args_list == [
        call(
            publishing_target="invenio",
            query_string=None,
            entity_type=["MergedContactPoint", "MergedOrganizationalUnit"],
            referenced_identifier=None,
            reference_field=None,
        )
    ]


@pytest.mark.usefixtures("mocked_backend")
def test_publisher_fallback_contact_identifiers() -> None:
    identifiers = publisher_fallback_contact_identifiers()
    assert identifiers == [MergedContactPointIdentifier("fakeFakeContact")]


@pytest.mark.usefixtures("mocked_backend")
def test_publisher_items(
    mocked_publisher_fallback_unit_identifiers_by_person: dict[
        MergedPersonIdentifier, list[MergedOrganizationalUnitIdentifier]
    ],
) -> None:
    container = cast(
        "PublisherItemsLike",
        publisher_items(
            publisher_items_without_actors(),
            publisher_persons(),
            publisher_contact_points_and_units(),
            publisher_fallback_contact_identifiers(),
            mocked_publisher_fallback_unit_identifiers_by_person,
        ),
    )
    assert len(container.items) == 4


@pytest.mark.parametrize(
    ("sink_name", "sink_class"),
    [
        pytest.param("s3", S3Sink),
        pytest.param("ndjson", NdjsonSink),
    ],
)
def test_publisher_sink_load(
    extracted_organization_rki: ExtractedOrganization,
    monkeypatch: pytest.MonkeyPatch,
    sink_name: str,
    sink_class: type[Any],
) -> None:
    settings = ExtractorsSettings.get()
    settings.publisher.sink = sink_name

    sink = MagicMock()
    sink.load.return_value = iter(())

    get_mock = MagicMock(return_value=sink)
    monkeypatch.setattr(sink_class, "get", get_mock)

    publisher_sink_load(ItemsContainer(items=[extracted_organization_rki]))

    get_mock.assert_called_once_with()
    sink.load.assert_called_once_with([extracted_organization_rki])
