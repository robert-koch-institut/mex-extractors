from unittest.mock import MagicMock

import pytest
from pytest import MonkeyPatch

from mex.extractors.open_data.connector import OpenDataConnector
from mex.extractors.open_data.models.source import (
    OpenDataParentResource,
    OpenDataResourceVersion,
    OpenDataVersionFiles,
)


def create_mocked_parent_response() -> dict:
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
                        "license": {"id": "no license"},
                    },
                    "files": [],
                },
            ],
            "total": 200,
        }
    }


def create_mocked_version_response() -> dict:
    return {
        "hits": {
            "hits": [
                {
                    "title": "Dumdidumdidum",
                    "id": 1001,
                    "conceptrecid": "Eins",
                    "metadata": {
                        "license": {"id": "cc-by-4.0"},
                        "contributors": [
                            {
                                "name": "Muster, Maxi",
                                "affiliation": "RKI",
                                "orcid": "1234567890",
                            },
                        ],
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
                    "created": "2021-01-01T01:01:01.111111+00:00",
                },
                {
                    "title": "Ladidadida",
                    "conceptrecid": "Eins",
                    "id": 1002,
                    "metadata": {
                        "license": {"id": "no license"},
                        "publication_date": "2022",
                        "creators": [
                            {"name": "Muster, Maxi"},
                            {"name": "Resolved, Roland"},
                        ],
                    },
                    "created": "2022-02-02T02:02:02.222222+00:00",
                },
            ],
            "total": 201,
        }
    }


def create_mocked_file_response() -> dict:
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

    mocked_version_response = create_mocked_version_response()
    resource_versions = [
        OpenDataResourceVersion.model_validate(
            mocked_version_response["hits"]["hits"][0]
        ),
        OpenDataResourceVersion.model_validate(
            mocked_version_response["hits"]["hits"][1]
        ),
    ]
    monkeypatch.setattr(
        OpenDataConnector, "get_resource_versions", lambda self, _: resource_versions
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
