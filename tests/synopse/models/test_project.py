from typing import Any

import pytest

from mex.extractors.synopse.models.project import SynopseProject


@pytest.mark.parametrize(
    ("contact", "expected"),
    [
        ("info@example.com", ["info@example.com"]),
        (["com@info.example"], ["com@info.example"]),
        (
            "info@example.com, example@info.com,com@example.info",
            ["info@example.com", "example@info.com", "com@example.info"],
        ),
    ],
)
def test_synopse_project_get_contacts(contact: Any, expected: list[str]) -> None:
    project = SynopseProject.model_validate(
        {
            "Studie": "STUDY",
            "StudienArtTyp": "TYPE",
            "StudienID": "ID",
            "Kontakt": contact,
        }
    )
    assert project.get_contacts() == expected
