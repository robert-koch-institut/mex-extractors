import pytest

from mex.extractors.open_data.extract import (
    extract_files_for_parent_resource,
    extract_oldest_record_version_creationdate,
    extract_open_data_persons_from_open_data_parent_resources,
    extract_parent_resources,
)
from mex.extractors.open_data.models.source import (
    OpenDataCreatorsOrContributors,
    OpenDataParentResource,
)


@pytest.mark.usefixtures("mocked_open_data")
def test_extract_parent_resources_mocked() -> None:
    open_data_sources = extract_parent_resources()

    assert isinstance(open_data_sources, list)

    assert open_data_sources[0].model_dump(exclude_defaults=True) == {
        "title": "Dumdidumdidum",
        "id": 1001,
        "conceptdoi": "10.3456/zenodo.7890",
        "conceptrecid": "Eins",
        "metadata": {
            "description": "<p>Test1</p> <br>\n<a href='test/2'>test3</a>",
            "license": {"id": "cc-by-4.0"},
            "contributors": [{"name": "Muster, Maxi", "orcid": "1234567890"}],
        },
        "files": [{"id": "file_test_id"}],
    }
    assert open_data_sources[1].model_dump(exclude_defaults=True) == {
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
    }


@pytest.mark.usefixtures("mocked_open_data")
def test_extract_oldest_record_version_creationdate(
    mocked_open_data_parent_resource: list[OpenDataParentResource],
) -> None:
    open_data_source_date = extract_oldest_record_version_creationdate(
        mocked_open_data_parent_resource
    )

    assert isinstance(open_data_source_date, str)

    assert open_data_source_date == "2021"


@pytest.mark.usefixtures("mocked_open_data")
def test_extract_files_for_parent_resource() -> None:
    open_data_source_files = extract_files_for_parent_resource(1001)

    assert isinstance(open_data_source_files, list)

    assert open_data_source_files[0].model_dump(exclude_defaults=True) == {
        "file_id": "file_test_id",
        "key": "some text",
        "links": {"self": "www.efg.hi"},
        "created": "2021-01-01T01:01:01.111111+00:00",
    }


def test_extract_open_data_persons_from_open_data_parent_resources(
    mocked_open_data_parent_resource: list[OpenDataParentResource],
) -> None:
    results = extract_open_data_persons_from_open_data_parent_resources(
        mocked_open_data_parent_resource
    )
    assert results == [
        OpenDataCreatorsOrContributors(
            name="Muster, Maxi",
            orcid="1234567890",
        )
    ]
