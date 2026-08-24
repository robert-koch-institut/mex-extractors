from unittest.mock import MagicMock

import pytest

from mex.common.exceptions import MExError
from mex.common.models import (
    AnyMergedModel,
    MergedOrganizationalUnit,
    MergedPrimarySource,
    PaginatedItemsContainer,
)
from mex.extractors.publisher.extract import (
    get_publishable_merged_item_by_identifier,
    get_publishable_merged_items,
)


@pytest.mark.usefixtures("mocked_backend_publisher")
def test_get_publishable_merged_items_mocked() -> None:
    items = get_publishable_merged_items()
    assert len(items) == 6
    assert items[0] == MergedPrimarySource(identifier="fakeFakeSource")


def test_get_publishable_merged_item_by_identifier(
    monkeypatch: pytest.MonkeyPatch,
    mocked_merged_organizational_units: list[MergedOrganizationalUnit],
) -> None:
    expected = mocked_merged_organizational_units[0]

    backend = MagicMock()
    backend.fetch_publishable_merged_items.return_value = PaginatedItemsContainer[
        AnyMergedModel
    ](
        total=1,
        items=[expected],
    )

    monkeypatch.setattr(
        "mex.extractors.publisher.extract.BackendApiConnector.get",
        MagicMock(return_value=backend),
    )

    result = get_publishable_merged_item_by_identifier(str(expected.identifier))

    assert result == expected
    backend.fetch_publishable_merged_items.assert_called_once()
    assert backend.fetch_publishable_merged_items.call_args.kwargs["identifier"] == str(
        expected.identifier
    )
    assert (
        backend.fetch_publishable_merged_items.call_args.kwargs["publishing_target"]
        == "invenio"
    )


@pytest.mark.parametrize(
    ("items", "expected_message"),
    [
        ([], "does not exist or is not publishable"),
        (
            [
                MergedPrimarySource(identifier="aaaaaaaaaaaaaa"),
                MergedPrimarySource(identifier="bbbbbbbbbbbbbb"),
            ],
            "More than one merged item found",
        ),
    ],
)
def test_get_publishable_merged_item_by_identifier_raises(
    monkeypatch: pytest.MonkeyPatch,
    items: list[AnyMergedModel],
    expected_message: str,
) -> None:
    """Raise if more or less than one item is found."""
    backend = MagicMock()
    backend.fetch_publishable_merged_items.return_value = PaginatedItemsContainer[
        AnyMergedModel
    ](
        total=len(items),
        items=items,
    )

    monkeypatch.setattr(
        "mex.extractors.publisher.extract.BackendApiConnector.get",
        MagicMock(return_value=backend),
    )

    with pytest.raises(MExError, match=expected_message):
        get_publishable_merged_item_by_identifier("some-id")
