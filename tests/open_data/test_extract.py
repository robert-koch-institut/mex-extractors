from collections.abc import Iterable

import pytest

from mex.extractors.open_data.extract import (
    extract_files_for_parent_resource,
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
        "title": "Dumdidumdidum",
        "id": 1001,
        "conceptdoi": "10.3456/zenodo.7890",
        "conceptrecid": "Eins",
        "metadata": {
            "description": "<p>Test1</p> <br>\n<a href='test/2'>test3</a>",
            "license": {"id": "cc-by-4.0"},
            "contributors": [{"name": "Muster, Maxi"}],
        },
        "files": [{"id": "file_test_id"}],
    }
    assert second_item.model_dump(exclude_defaults=True) == {
        "title": "This is a test",
        "conceptrecid": "Zwei",
        "id": 2002,
        "metadata": {
            "creators": [{"name": "Muster, Maxi"}],
            "license": {"id": "no license"},
        },
        "files": [],
    }


@pytest.mark.usefixtures("mocked_open_data")
def test_extract_resource_versions_mocked(
    mocked_open_data_parent_resource: Iterable[OpenDataParentResource],
) -> None:
    open_data_sources = list(
        extract_resource_versions(mocked_open_data_parent_resource)
    )

    assert isinstance(open_data_sources, Iterable)

    assert len(open_data_sources) == 2
    assert open_data_sources[0].model_dump(exclude_defaults=True) == {
        "id": 1001,
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
        "created": "2021-01-01T01:01:01.111111+00:00",
    }
    assert open_data_sources[1].model_dump(exclude_defaults=True) == {
        "id": 1002,
        "metadata": {
            "license": {"id": "no license"},
            "publication_date": "2022",
            "creators": [
                {"name": "Muster, Maxi"},
                {"name": "Pattern, Pepa"},
            ],
        },
        "created": "2022-02-02T02:02:02.222222+00:00",
    }


@pytest.mark.usefixtures("mocked_open_data")
def test_extract_oldest_record_version_creationdate(
    mocked_open_data_parent_resource: Iterable[OpenDataParentResource],
) -> None:
    open_data_source_date = extract_oldest_record_version_creationdate(
        mocked_open_data_parent_resource
    )

    assert isinstance(open_data_source_date, str)

    assert open_data_source_date == "2021"


@pytest.mark.usefixtures("mocked_open_data")
def test_extract_files_for_parent_resource() -> None:
    open_data_source_files = list(extract_files_for_parent_resource(1001))

    assert isinstance(open_data_source_files, Iterable)

    assert open_data_source_files[0].model_dump(exclude_defaults=True) == {
        "file_id": "file_test_id",
        "key": "some text",
        "links": {"self": "www.efg.hi"},
        "created": "2021-01-01T01:01:01.111111+00:00",
    }
