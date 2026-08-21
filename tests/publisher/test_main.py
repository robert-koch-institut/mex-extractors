from typing import TYPE_CHECKING, Any, cast
from unittest.mock import MagicMock, call

import pytest

from mex.common.exceptions import MExError
from mex.common.models import (
    AnyMergedModel,
    ExtractedOrganization,
    ItemsContainer,
    MergedBibliographicResource,
    MergedOrganizationalUnit,
    MergedPerson,
)
from mex.common.types import (
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedPersonIdentifier,
)
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.publisher.main import (
    publisher_contact_points_and_units,
    publisher_csv_load,
    publisher_fallback_contact_identifiers,
    publisher_items,
    publisher_items_without_actors,
    publisher_persons,
    publisher_sink_load,
)
from mex.extractors.publisher.models import BibliographicResourceForCsv
from mex.extractors.settings import ExtractorsSettings
from mex.extractors.sinks.ndjson import NdjsonSink
from mex.extractors.sinks.s3 import S3Sink

if TYPE_CHECKING:
    from mex.extractors.publisher.models import PublisherItemsLike


@pytest.mark.usefixtures(
    "mocked_backend_publisher",
    "mocked_s3sink_client",  # needed for hardcoded upload to S3. Remove with MX-1808
    "mocked_wikidata",
)
def test_run(
    monkeypatch: pytest.MonkeyPatch,
    mocked_merged_organizational_units: list[MergedOrganizationalUnit],
) -> None:
    merged_items_by_identifier: dict[str, AnyMergedModel] = {
        str(unit.identifier): unit for unit in mocked_merged_organizational_units
    }
    merged_items_by_identifier["PersonIdentifier"] = MergedPerson(
        identifier="PersonIdentifier",
        fullName=["Dr. Test Person"],
    )

    def mocked_get_publishable_merged_item_by_identifier(
        identifier: object,
    ) -> AnyMergedModel:
        if item := merged_items_by_identifier.get(str(identifier)):
            return item

        msg = f"Unexpected identifier lookup in test_run: {identifier!r}"
        raise MExError(msg)

    monkeypatch.setattr(
        "mex.extractors.publisher.transform.get_publishable_merged_item_by_identifier",
        mocked_get_publishable_merged_item_by_identifier,
    )
    assert run_job_in_process("publisher")


@pytest.mark.usefixtures("mocked_backend_publisher")
def test_publisher_items_without_actors(mocked_backend_publisher: MagicMock) -> None:
    container = cast("PublisherItemsLike", publisher_items_without_actors())
    assert len(container.items) == 1
    assert isinstance(container.items[0], MergedBibliographicResource)
    assert (
        mocked_backend_publisher.fetch_all_publishable_merged_items.call_args_list
        == [
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
                reference_filters=None,
            ),
        ]
    )


@pytest.mark.usefixtures("mocked_backend_publisher")
def test_publisher_persons(mocked_backend_publisher: MagicMock) -> None:
    container = cast("PublisherItemsLike", publisher_persons())
    assert len(container.items) == 1
    assert (
        mocked_backend_publisher.fetch_all_publishable_merged_items.call_args_list
        == [
            call(
                publishing_target="invenio",
                query_string=None,
                entity_type=["MergedPerson"],
                reference_filters=None,
            ),
            call(
                publishing_target="invenio",
                query_string=None,
                entity_type=["MergedConsent"],
                reference_filters=None,
            ),
        ]
    )


def test_publisher_contact_points_and_units(
    mocked_backend_publisher: MagicMock,
) -> None:
    container = cast("PublisherItemsLike", publisher_contact_points_and_units())
    assert len(container.items) == 2
    assert (
        mocked_backend_publisher.fetch_all_publishable_merged_items.call_args_list
        == [
            call(
                publishing_target="invenio",
                query_string=None,
                entity_type=["MergedContactPoint", "MergedOrganizationalUnit"],
                reference_filters=None,
            )
        ]
    )


@pytest.mark.usefixtures("mocked_backend_publisher")
def test_publisher_fallback_contact_identifiers() -> None:
    identifiers = publisher_fallback_contact_identifiers()
    assert identifiers == [MergedContactPointIdentifier("fakeFakeContact")]


@pytest.mark.usefixtures("mocked_backend_publisher")
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


def test_publisher_csv_load_sorts_by_publication_year(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test sorting of Publications by Year (newest to oldest)."""
    older = BibliographicResourceForCsv(
        contributingUnit=["FG 1"],
        publicationYear="1999",
        creator=["A"],
        title=["Older"],
        journal=[],
        doi=None,
        accessRestriction="open",
        publisher=[],
    )
    newer = BibliographicResourceForCsv(
        contributingUnit=["FG 1"],
        publicationYear="2022",
        creator=["B"],
        title=["Newer"],
        journal=[],
        doi=None,
        accessRestriction="open",
        publisher=[],
    )

    sink = MagicMock()
    sink.load.return_value = iter(())

    monkeypatch.setattr(
        "mex.extractors.publisher.main.S3CsvSink",
        MagicMock(return_value=sink),
    )

    publisher_csv_load({"FG 1": [older, newer]})

    sink.load.assert_called_once_with(
        [newer, older],
        unit_name="FG 1",
    )
