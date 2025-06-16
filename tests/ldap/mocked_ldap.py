from unittest.mock import MagicMock
from uuid import UUID

import pytest
from pytest import MonkeyPatch

from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models import LDAPActor, LDAPPerson


@pytest.fixture
def mocked_ldap(monkeypatch: MonkeyPatch) -> None:
    """Mock the LDAP connector to return resolved persons and units."""
    monkeypatch.setattr(
        LDAPConnector,
        "__init__",
        lambda self: setattr(self, "_connection", MagicMock()),
    )
    monkeypatch.setattr(
        LDAPConnector,
        "get_functional_accounts",
        lambda *_, **__: [
            LDAPActor(
                sAMAccountName="ContactC",
                objectGUID=UUID(int=4, version=4),
                mail=["email@email.de", "contactc@rki.de"],
            ),
        ],
    )
    monkeypatch.setattr(
        LDAPConnector,
        "get_persons",
        lambda *_, **__: [
            LDAPPerson(
                employeeID="42",
                sn="Resolved",
                givenName=["Roland"],
                displayName="Resolved, Roland",
                objectGUID=UUID(int=1, version=4),
                department="PARENT-UNIT",
                mail=["test_person@email.de"],
            )
        ],
    )
