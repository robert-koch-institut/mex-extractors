from unittest.mock import MagicMock

import pytest
from pytest import MonkeyPatch

from mex.extractors.open_data.connector import OpenDataConnector
from mex.extractors.open_data.models.source import (
    OpenDataParentResource,
    OpenDataResourceVersion,
)


def create_mocked_parent_repsonse() -> dict:
    return {
        "hits": {
            "hits": [
                {
                    "id": 1001,
                    "conceptrecid": "Eins",
                    "metadata": {
                        "license": {"id": "cc-by-4.0"},
                        "contributors": [{"name": "Muster, Maxi"}],
                    },
                },
                {
                    "title": "This is a test",
                    "conceptrecid": "Zwei",
                    "id": 2002,
                    "metadata": {
                        "description": "<p> This is a test</p> <br>/n<a href> and </a>",
                        "creators": [{"name": "Muster, Maxi"}],
                        "license": {"id": "no license"},
                    },
                    "conceptdoi": "12.3456/zenodo.7890",
                },
                {
                    "id": 3003,
                    "conceptrecid": "three",
                    "metadata": {
                        "license": {"id": "cc-by-4.0"},
                        "creators": [{"name": "Pattern, Pepa"}],
                    },
                },
            ],
            "total": 200,
        }
    }


def create_mocked_version_repsonse() -> dict:
    return {
        "hits": {
            "hits": [
                {
                    "id": 1001,
                    "conceptrecid": "Eins",
                    "metadata": {
                        "license": {"id": "cc-by-4.0"},
                        "contributors": [{"name": "Muster, Maxi"}],
                        "related_identifiers": [
                            {
                                "identifier": "should be transformed",
                                "relation": "isDocumentedBy",
                            },
                            {
                                "identifier": "should be extracted but NOT transformed",
                                "relation": "isSupplementTo",
                            },
                        ],
                        "publication_date": "2021",
                    },
                    "files": [{"id": "file 1"}, {"id": "file 2"}, {"id": "file 3"}],
                },
                {
                    "title": "Ladidadida",
                    "conceptrecid": "Eins",
                    "id": 1002,
                    "metadata": {
                        "license": {"id": "no license"},
                        "publication_date": "2022",
                    },
                    "conceptdoi": "12.3456/zenodo.7890",
                    "files": [],
                },
                {
                    "id": 1003,
                    "conceptrecid": "Eins",
                    "metadata": {
                        "license": {"id": "cc-by-4.0"},
                        "creators": [{"name": "Pattern, Pepa"}],
                        "publication_date": "2023",
                    },
                    "files": [{"id": "A"}],
                },
            ],
            "total": 201,
        }
    }


def create_mocked_file_response() -> dict:
    return {
        "entries": [
            {"file_id": "file 1", "key": "some text", "links": {"self": "www.fge.hi"}},
            {"file_id": "file 2", "links": {"self": "www.abc.de"}},
            {"file_id": "file 3", "key": "more text", "links": {"self": "jklm.no"}},
        ],
    }


@pytest.fixture
def mocked_open_data(monkeypatch: MonkeyPatch) -> None:
    """Mock the Open data connector to return dummy resources."""
    mocked_parent_response = create_mocked_parent_repsonse()
    parent_resources = (
        OpenDataParentResource.model_validate(
            mocked_parent_response["hits"]["hits"][0]
        ),
        OpenDataParentResource.model_validate(
            mocked_parent_response["hits"]["hits"][1]
        ),
    )

    monkeypatch.setattr(
        OpenDataConnector, "get_parent_resources", lambda _: iter(parent_resources)
    )

    mocked_version_repsonse = create_mocked_version_repsonse()
    resource_versions = (
        OpenDataResourceVersion.model_validate(
            mocked_version_repsonse["hits"]["hits"][0]
        ),
        OpenDataResourceVersion.model_validate(
            mocked_version_repsonse["hits"]["hits"][1]
        ),
    )
    monkeypatch.setattr(
        OpenDataConnector,
        "get_resource_versions",
        lambda self, _: iter(resource_versions),
    )

    def __init__(self: OpenDataConnector) -> None:
        self.session = MagicMock()
        self.url = "https://mock-opendata"

    monkeypatch.setattr(OpenDataConnector, "__init__", __init__)
