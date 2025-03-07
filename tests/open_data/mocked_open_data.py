def create_mocked_parent_response() -> dict:
    return {
        "hits": {
            "hits": [
                {
                    "title": "Dumdidumdidum",
                    "id": 1001,
                    "conceptrecid": "Eins",
                    "metadata": {
                        "description": "<p>Test1</p> <br>\n<a href='test/2'>test3</a>",
                        "license": {"id": "cc-by-4.0"},
                        "contributors": [{"name": "Muster, Maxi"}],
                    },
                },
                {
                    "title": "This is a test",
                    "conceptrecid": "Zwei",
                    "id": 2002,
                    "metadata": {
                        "creators": [{"name": "Muster, Maxi"}],
                        "license": {"id": "no license"},
                    },
                    "conceptdoi": "12.3456/zenodo.7890",
                },
                {
                    "title": "lorem",
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
                    "files": [{"id": "file_test_id"}],
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
                            {"name": "Pattern, Pepa"},
                        ],
                    },
                    "created": "2022-02-02T02:02:02.222222+00:00",
                    "files": [],
                },
                {
                    "title": "Dideldideldei",
                    "id": 1003,
                    "conceptrecid": "Eins",
                    "metadata": {
                        "license": {"id": "cc-by-4.0"},
                        "creators": [{"name": "Pattern, Pepa"}],
                        "publication_date": "2023",
                    },
                    "created": "2023-03-03T03:03:03.333333+00:00",
                    "files": [{"id": "file 1"}, {"id": "file 2"}, {"id": "file 3"}],
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
