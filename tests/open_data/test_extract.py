from collections.abc import Iterable

import pytest

from mex.extractors.open_data.extract import (
    extract_parent_resources,
    extract_resource_versions,
)


@pytest.mark.usefixtures("mocked_open_data")
def test_extract_parent_resources_mocked() -> None:
    open_data_sources = extract_parent_resources()

    assert isinstance(open_data_sources, Iterable)
    first_item, second_item = tuple(open_data_sources)

    assert first_item.model_dump(exclude_defaults=True) == {
        "id": 1,
        "conceptrecid": "Eins",
    }
    assert second_item.model_dump(exclude_defaults=True) == {
        "id": 2,
        "conceptrecid": "Zwei",
        "metadata": {
            "title": "This is a test",
            "description": "<p> This is a test</p> <br>/n<a href> and </a>",
            "creators": [{"name": "Muster, Maxi"}],
        },
        "conceptdoi": "12.3456/zenodo.7890",
    }


@pytest.mark.usefixtures("mocked_open_data")
def test_extract_resource_versions_mocked() -> None:
    open_data_sources = list(extract_resource_versions())

    assert isinstance(open_data_sources, Iterable)

    assert len(open_data_sources) == 4  # 2 versions per each of 2 parents
    assert open_data_sources[0].model_dump(exclude_defaults=True) == {
        "id": 1,
        "conceptrecid": "Eins",
    }
    assert open_data_sources[1].model_dump(exclude_defaults=True) == {
        "id": 2,
        "conceptrecid": "Eins",
        "metadata": {
            "license": {"id": "cc-by-4.0"},
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
        },
    }
