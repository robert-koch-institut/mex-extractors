from unittest.mock import MagicMock

import pytest
import requests
from pytest import MonkeyPatch

from mex.common.exceptions import MExError
from mex.extractors.drop import DropApiConnector
from mex.extractors.seq_repo.extract import extract_sources


@pytest.mark.usefixtures(
    "mocked_drop",
)
def test_extract_sources() -> None:
    sources = list(extract_sources())
    expected = {
        "project_coordinators": ["max", "mustermann", "yee-haw"],
        "customer_org_unit_id": "FG99",
        "sequencing_date": "2023-08-07",
        "lims_sample_id": "test-sample-id",
        "sequencing_platform": "TEST",
        "species": "Severe acute respiratory syndrome coronavirus 2",
        "project_name": "FG99-ABC-123",
        "customer_sample_name": "test-customer-name-1",
        "project_id": "TEST-ID",
    }
    assert sources[0].model_dump() == expected


def test_extract_sources_fails_on_unexpected_number_of_files(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        DropApiConnector,
        "__init__",
        lambda self: setattr(self, "session", MagicMock(spec=requests.Session)),
    )
    monkeypatch.setattr(
        DropApiConnector,
        "list_files",
        lambda _self, x_system: [],
    )

    with pytest.raises(MExError, match="Expected exactly one seq-repo file"):
        list(extract_sources())
