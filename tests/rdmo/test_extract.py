from collections.abc import Iterable

import pytest

from mex.extractors.rdmo.extract import extract_rdmo_sources


@pytest.mark.usefixtures("mocked_rdmo")
def test_extract_rdmo_sources_mocked() -> None:
    rdmo_sources = extract_rdmo_sources()

    assert isinstance(rdmo_sources, Iterable)
    first_item, second_item = tuple(rdmo_sources)

    assert first_item.model_dump(exclude_defaults=True) == {"id": 1, "title": "Unowned"}
    assert second_item.model_dump(exclude_defaults=True) == {
        "id": 2,
        "owners": [
            {
                "email": "musterm@rki.de",
                "first_name": "Max",
                "id": 100,
                "last_name": "Muster",
                "username": "musterm",
            },
            {
                "email": "beispielb@rki.de",
                "first_name": "Beate",
                "id": 200,
                "last_name": "Beispiel",
                "username": "beispielb",
            },
        ],
        "title": "Owned",
        "question_answer_pairs": {
            "/questions/project_proposal/general/project-title/acronym": "LI1",
            "/questions/project_proposal/general/project-title/project_type": "rki-internal_Project",
            "/questions/project_proposal/general/project-title/title": "Lorem Ipsum",
            "questions/project_proposal/general/project-schedule-schedule/project_end": "2006-05",
            "questions/project_proposal/general/project-schedule-schedule/project_start": "2004",
        },
    }
