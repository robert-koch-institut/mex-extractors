from collections.abc import Iterable
from unittest.mock import MagicMock
from uuid import UUID

import pytest
import requests
from pytest import MonkeyPatch

from mex.common.exceptions import MExError
from mex.common.ldap.models import LDAPPerson, LDAPPersonWithQuery
from mex.extractors.drop import DropApiConnector
from mex.extractors.seq_repo.extract import (
    extract_source_project_coordinator,
    extract_sources,
)
from mex.extractors.seq_repo.filter import filter_sources_on_latest_sequencing_date
from mex.extractors.seq_repo.model import SeqRepoSource


@pytest.mark.usefixtures("mocked_drop")
def test_extract_sources() -> None:
    sources = extract_sources()
    expected = {
        "project_coordinators": ["max", "mustermann", "yee-haw"],
        "customer_org_unit_id": "FG99",
        "sequencing_date": "2023-08-07",
        "lims_sample_id": "test-sample-id",
        "sequencing_platform": "TEST",
        "species": "virus XYZ",
        "project_name": "FG99-ABC-123",
        "customer_sample_name": "test-customer-name-1",
        "project_id": "TEST-ID",
    }
    assert len(sources) == 2
    assert sources[0].model_dump() == expected


def test_extract_sources_fails_on_unexpected_number_of_files(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        DropApiConnector,
        "__init__",
        lambda self: setattr(self, "session", MagicMock(spec=requests.Session)),
    )
    monkeypatch.setattr(
        DropApiConnector,
        "list_files",
        lambda _self, x_system: [],
    )

    with pytest.raises(MExError, match=r"Expected exactly one seq-repo file"):
        extract_sources()


@pytest.mark.usefixtures("mocked_ldap")
def test_extract_source_project_coordinator(
    seq_repo_sources: Iterable[SeqRepoSource],
) -> None:
    seq_repo_sources_dict = filter_sources_on_latest_sequencing_date(seq_repo_sources)
    project_coordinators = extract_source_project_coordinator(seq_repo_sources_dict)

    assert project_coordinators == [
        LDAPPersonWithQuery(
            person=LDAPPerson(
                sAMAccountName=None,
                objectGUID=UUID("00000000-0000-4000-8000-000000000001"),
                mail=["test_person@email.de"],
                company=None,
                department="PARENT-UNIT",
                departmentNumber=None,
                displayName="Resolved, Roland",
                employeeID="42",
                givenName=["Roland"],
                ou=[],
                sn="Resolved",
            ),
            query="max",
        ),
        LDAPPersonWithQuery(
            person=LDAPPerson(
                sAMAccountName=None,
                objectGUID=UUID("00000000-0000-4000-8000-000000000001"),
                mail=["test_person@email.de"],
                company=None,
                department="PARENT-UNIT",
                departmentNumber=None,
                displayName="Resolved, Roland",
                employeeID="42",
                givenName=["Roland"],
                ou=[],
                sn="Resolved",
            ),
            query="mustermann",
        ),
    ]
