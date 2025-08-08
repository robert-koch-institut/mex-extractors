from unittest.mock import MagicMock

import pytest

from mex.extractors.datenkompass.load import write_items_to_csv
from mex.extractors.datenkompass.models.item import DatenkompassActivity


@pytest.mark.usefixtures("mocked_boto")
def test_write_item_to_csv(
    mocked_datenkompass_activity: list[DatenkompassActivity],
    mocked_boto: MagicMock,
) -> None:
    write_items_to_csv(mocked_datenkompass_activity, mocked_boto)

    mocked_boto.put_object.assert_called_once()
    call_kwargs = mocked_boto.put_object.call_args.kwargs
    assert call_kwargs["Key"] == "datenkompass_MergedActivity.csv"
