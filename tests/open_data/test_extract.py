from collections.abc import Iterable

import pytest

from mex.extractors.open_data.extract import (
    extract_files_for_resource_version,
    extract_oldest_record_version_creationdate,
    extract_parent_resources,
    extract_resource_versions,
)
from mex.extractors.open_data.models.source import (
    OpenDataParentResource,
)


@pytest.mark.usefixtures("mocked_open_data")
def test_extract_parent_resources_mocked() -> None:
    open_data_sources = extract_parent_resources()

    assert isinstance(open_data_sources, Iterable)
    first_item, second_item = tuple(open_data_sources)

    assert first_item.model_dump(exclude_defaults=True) == {
        "id": 1001,
        "conceptrecid": "Eins",
        "metadata": {
            "license": {"id": "cc-by-4.0"},
            "contributors": [{"name": "Muster, Maxi"}],
        },
    }
    assert second_item.model_dump(exclude_defaults=True) == {
        "title": "This is a test",
        "conceptrecid": "Zwei",
        "id": 2002,
        "metadata": {
            "description": "<p> This is a test</p> <br>/n<a href> and </a>",
            "creators": [{"name": "Muster, Maxi"}],
            "license": {"id": "no license"},
        },
        "conceptdoi": "12.3456/zenodo.7890",
    }


@pytest.mark.usefixtures("mocked_open_data")
def test_extract_resource_versions_mocked(
    mocked_parent_resource_reponse: Iterable[OpenDataParentResource],
) -> None:
    open_data_sources = list(extract_resource_versions(mocked_parent_resource_reponse))

    assert isinstance(open_data_sources, Iterable)

    assert len(open_data_sources) == 2
    assert open_data_sources[0].model_dump(exclude_defaults=True) == {
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
    }
    assert open_data_sources[1].model_dump(exclude_defaults=True) == {
        "title": "Ladidadida",
        "conceptrecid": "Eins",
        "id": 1002,
        "metadata": {
            "license": {"id": "no license"},
            "publication_date": "2022",
        },
        "files": [],
    }


@pytest.mark.usefixtures("mocked_open_data")
def test_extract_oldest_record_version_creationdate(
    mocked_parent_resource_reponse: Iterable[OpenDataParentResource],
) -> None:
    open_data_source_date = extract_oldest_record_version_creationdate(
        mocked_parent_resource_reponse
    )

    assert isinstance(open_data_source_date, str)

    assert open_data_source_date == "2021"


@pytest.mark.usefixtures("mocked_open_data")
def test_extract_files_for_resource_version() -> None:
    open_data_source_files = list(extract_files_for_resource_version(1001))

    assert isinstance(open_data_source_files, Iterable)

    assert open_data_source_files[0].model_dump(exclude_defaults=True) == {
        "file_id": "file 1",
        "key": "some text",
        "links": {"self": "www.fge.hi"},
    }
    assert open_data_source_files[1].model_dump(exclude_defaults=True) == {
        "file_id": "file 2",
        "links": {"self": "www.abc.de"},
    }
