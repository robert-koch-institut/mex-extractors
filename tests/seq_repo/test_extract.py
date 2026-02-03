from collections.abc import Iterable
from unittest.mock import MagicMock

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
        "project_coordinators": ["test_person", "test_person"],
        "customer_org_unit_id": "FG99",
        "sequencing_date": "2023-08-07",
        "lims_sample_id": "test-sample-id",
        "sequencing_platform": "TEST",
        "species": "virus XYZ",
        "project_name": "FG99-ABC-123",
        "customer_sample_name": "test-customer-name-1",
        "project_id": "TEST-ID",
    }
    assert len(sources) == 4
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
    ldap_frieda_fictitious: LDAPPerson,
    ldap_roland_resolved: LDAPPerson,
) -> None:
    seq_repo_sources_dict = filter_sources_on_latest_sequencing_date(seq_repo_sources)
    project_coordinators = extract_source_project_coordinator(seq_repo_sources_dict)

    assert project_coordinators == [
        LDAPPersonWithQuery(
            person=ldap_frieda_fictitious,
            query="FictitiousF",
        ),
        LDAPPersonWithQuery(
            person=ldap_roland_resolved,
            query="ResolvedR",
        ),
    ]
