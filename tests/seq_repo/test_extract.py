from typing import TYPE_CHECKING
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

if TYPE_CHECKING:
    from mex.extractors.seq_repo.model import SeqRepoSource


@pytest.mark.usefixtures("mocked_drop")
def test_extract_sources() -> None:
    sources = extract_sources()
    expected = {
        "project_coordinators": ["test_person", "test_person"],
        "basepair_count": 1,
        "customer_org_unit_id": "FG99",
        "sequencing_date": "2023-08-07",
        "lims_sample_id": "test-sample-id",
        "sequencing_platform": "TEST",
        "species": "virus XYZ",
        "project_name": "FG99-ABC-123",
        "customer_sample_name": "test-customer-name-1",
        "project_id": "TEST-ID",
        "reads_count": 2,
    }
    assert len(sources) == 4
    assert sources[0].model_dump(exclude_defaults=True) == expected


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
    seq_repo_sources: list[SeqRepoSource],
) -> None:
    project_coordinators = extract_source_project_coordinator(seq_repo_sources)

    assert project_coordinators == [
        LDAPPersonWithQuery(
            person=LDAPPerson(
                objectGUID=UUID("00000000-0000-4000-8000-000000000003"),
                sAMAccountName="FictitiousF",
                mail=["fictitiousf@rki.de"],
                employeeID="71",
                givenName=["Frieda"],
                sn="Fictitious",
                company=None,
                department="FG99",
                departmentNumber=None,
                displayName="Fictitious, Frieda, Dr.",
                ou=[],
            ),
            query="FictitiousF",
        ),
        LDAPPersonWithQuery(
            person=LDAPPerson(
                objectGUID=UUID("00000000-0000-4000-8000-000000000001"),
                sAMAccountName="ResolvedR",
                mail=["resolvedr@rki.de"],
                employeeID="42",
                givenName=["Roland"],
                sn="Resolved",
                company=None,
                department="parent-unit",
                departmentNumber="FG99",
                displayName="Resolved, Roland",
                ou=[],
            ),
            query="ResolvedR",
        ),
        LDAPPersonWithQuery(
            person=LDAPPerson(
                objectGUID=UUID("00000000-0000-4000-8000-000000000002"),
                sAMAccountName="FelicitasJ",
                mail=["felicitasj@rki.de"],
                employeeID="70",
                givenName=["Juturna"],
                sn="Felicitás",
                company=None,
                department="FG99",
                departmentNumber=None,
                displayName="Felicitás, Juturna",
                ou=[],
            ),
            query="FelicitasJ",
        ),
        LDAPPersonWithQuery(
            person=LDAPPerson(
                objectGUID=UUID("00000000-0000-4000-8000-000000000003"),
                sAMAccountName="FictitiousF",
                mail=["fictitiousf@rki.de"],
                employeeID="71",
                givenName=["Frieda"],
                sn="Fictitious",
                company=None,
                department="FG99",
                departmentNumber=None,
                displayName="Fictitious, Frieda, Dr.",
                ou=[],
            ),
            query="NonExistent",
        ),
    ]
