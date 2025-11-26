import json
from io import BytesIO
from typing import Any
from unittest.mock import MagicMock, Mock
from zipfile import ZIP_DEFLATED, ZipFile

import pytest
import requests
from pytest import MonkeyPatch

from mex.extractors.open_data import transform
from mex.extractors.open_data.connector import OpenDataConnector
from mex.extractors.open_data.models.source import (
    OpenDataParentResource,
    OpenDataVersionFiles,
)


def create_mocked_parent_response() -> dict[str, Any]:
    return {
        "hits": {
            "hits": [
                {
                    "title": "Dumdidumdidum",
                    "id": 1001,
                    "conceptrecid": "Eins",
                    "conceptdoi": "10.3456/zenodo.7890",
                    "metadata": {
                        "description": "<p>Test1</p> <br>\n<a href='test/2'>test3</a>",
                        "license": {"id": "cc-by-4.0"},
                        "contributors": [
                            {"name": "Muster, Maxi", "orcid": "1234567890"}
                        ],
                        "resource_type": {"name": "test1", "type": "dataset"},
                    },
                    "files": [{"id": "file_test_id"}],
                },
                {
                    "title": "This is a test",
                    "conceptrecid": "Zwei",
                    "id": 2002,
                    "metadata": {
                        "creators": [
                            {
                                "name": "Muster, Maxi",
                                "affiliation": "RKI",
                                "orcid": "1234567890",
                            },
                            {
                                "name": "Resolved, Roland",
                                "affiliation": "Universität",
                                "orcid": "9876543210",
                            },
                        ],
                        "resource_type": {"name": "test2", "type": "something else"},
                        "license": {"id": "no license"},
                    },
                    "files": [],
                },
            ],
            "total": 42,
        }
    }


def create_mocked_file_response() -> dict[str, Any]:
    return {
        "entries": [
            {
                "file_id": "file_test_id",
                "key": "some text",
                "links": {"self": "www.efg.hi"},
                "created": "2021-01-01T01:01:01.111111+00:00",
            },
        ],
    }


@pytest.fixture
def mocked_open_data(monkeypatch: MonkeyPatch) -> None:
    """Mock the Open data connector to return dummy resources."""
    mocked_parent_response = create_mocked_parent_response()
    parent_resources = [
        OpenDataParentResource.model_validate(
            mocked_parent_response["hits"]["hits"][0]
        ),
        OpenDataParentResource.model_validate(
            mocked_parent_response["hits"]["hits"][1]
        ),
    ]
    monkeypatch.setattr(
        OpenDataConnector, "get_parent_resources", lambda _: parent_resources
    )

    monkeypatch.setattr(
        OpenDataConnector,
        "get_oldest_resource_version_creation_date",
        lambda self, _: "2021",
    )

    mocked_file_response = create_mocked_file_response()
    version_files = [
        OpenDataVersionFiles.model_validate(mocked_file_response["entries"][0]),
    ]
    monkeypatch.setattr(
        OpenDataConnector,
        "get_files_for_resource_version",
        lambda self, _: version_files,
    )

    def __init__(self: OpenDataConnector) -> None:
        self.session = MagicMock()
        self.url = "https://mock-opendata"

    monkeypatch.setattr(OpenDataConnector, "__init__", __init__)

    # TODO @MX-2075: remove
    monkeypatch.setattr(transform, "FALLBACK_UNIT", "FG 99")

    # Create a mocked ZIP file with according folder structure, naming, and content."""
    json_contents = [
        {
            "fields": [
                {
                    "name": "Foo1",
                    "type": "string",
                    "description": "amaze",
                    "constraints": {"enum": ["a", "b"]},
                    "categories": [
                        {"value": "a", "label": "the letter 'a'"},
                        {"value": "b", "label": "and also 'b'"},
                    ],
                },
                {
                    "name": "Foo2",
                    "type": "string",
                    "description": "wow",
                    "constraints": {"enum": ["c", "d", "e", "f", "g"]},
                },
            ],
        },
        {
            "fields": [
                {
                    "name": "Bar",
                    "type": "integer",
                    "description": "no constraints and no categories",
                    "something_irrelevant": True,
                }
            ]
        },
    ]
    tableschema_names = ["tableschema_foo.json", "tableschema_bar.json"]

    buffer = BytesIO()
    with ZipFile(buffer, mode="w", compression=ZIP_DEFLATED) as zf:
        for i in range(len(tableschema_names)):
            filename = f"Metadaten/schemas/{tableschema_names[i]}"
            zf.writestr(filename, json.dumps(json_contents[i]))

    zip_response = Mock(spec=requests.Response)
    zip_response.status_code = 200
    zip_response.content = buffer.getvalue()

    mock_method = MagicMock(return_value=zip_response)
    monkeypatch.setattr(OpenDataConnector, "get_schema_zipfile", mock_method)
