from unittest.mock import MagicMock

from mex.extractors.datenkompass.load import write_item_to_json
from tests.datenkompass.mocked_item_lists import (
    mocked_datenkompass_activity,
)


def test_write_item_to_json() -> None:
    mock_s3 = MagicMock()
    datenkompassitems = mocked_datenkompass_activity()

    write_item_to_json(datenkompassitems, mock_s3)
    mock_s3.put_object.assert_called_once()
    call_kwargs = mock_s3.put_object.call_args.kwargs
    assert call_kwargs["Key"] == "datenkompass_MergedActivity.json"
