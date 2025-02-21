from pathlib import Path
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from pytest import MonkeyPatch

from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models.actor import LDAPActor
from mex.common.ldap.models.person import LDAPPerson
from mex.common.models import ExtractedOrganization
from mex.common.types import MergedPrimarySourceIdentifier
from mex.extractors.settings import Settings

pytest_plugins = (
    "mex.common.testing.plugin",
    "tests.blueant.mocked_blueant",
    "tests.confluence_vvt.mocked_confluence_vvt",
    "tests.datscha_web.mocked_datscha_web",
    "tests.grippeweb.mocked_grippeweb",
    "tests.ifsg.mocked_ifsg",
    "tests.open_data.mocked_open_data",
    "tests.rdmo.mocked_rdmo",
    "tests.drop.mocked_drop",
)

TEST_DATA_DIR = Path(__file__).parent / "test_data"


@pytest.fixture(autouse=True)
def settings() -> Settings:
    """Load the settings for this pytest session."""
    return Settings.get()


@pytest.fixture
def extracted_organization_rki() -> ExtractedOrganization:
    return ExtractedOrganization(
        identifierInPrimarySource="Robert Koch-Institut",
        hadPrimarySource=MergedPrimarySourceIdentifier.generate(123),
        officialName=["Robert Koch-Institut"],
    )


@pytest.fixture
def mocked_ldap(monkeypatch: MonkeyPatch) -> None:
    """Mock the LDAP connector to return resolved persons and units."""
    actors = [
        LDAPActor(
            sAMAccountName="ContactC",
            objectGUID=UUID(int=4, version=4),
            mail=["email@email.de", "contactc@rki.de"],
        ),
    ]
    monkeypatch.setattr(
        LDAPConnector,
        "__init__",
        lambda self: setattr(self, "_connection", MagicMock()),
    )
    monkeypatch.setattr(
        LDAPConnector, "get_functional_accounts", lambda *_, **__: iter(actors)
    )
    persons = [
        LDAPPerson(
            employeeID="42",
            sn="Resolved",
            givenName=["Roland"],
            displayName="Resolved, Roland",
            objectGUID=UUID(int=1, version=4),
            department="PARENT-UNIT",
            mail=["test_person@email.de"],
        )
    ]
    monkeypatch.setattr(
        LDAPConnector,
        "get_persons",
        lambda *_, **__: iter(persons),
    )
