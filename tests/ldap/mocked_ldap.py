import os
from unittest.mock import MagicMock
from uuid import UUID

import pytest
from pytest import MonkeyPatch

from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models import LDAPActor, LDAPPerson


@pytest.fixture(params=["ldap_patched_connector", "ldap_mock_server"])
def mocked_ldap(request: pytest.FixtureRequest, monkeypatch: MonkeyPatch) -> None:
    """Run each test with patched connector and/or against a mock server."""
    if request.param == "ldap_patched_connector":
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
                    sAMAccountName="test_person",
                    employeeID="42",
                    sn="Resolved",
                    givenName=["Roland"],
                    displayName="Resolved, Roland",
                    objectGUID=UUID(int=1, version=4),
                    department="PARENT-UNIT",
                    departmentNumber="FG99",
                    mail=["test_person@email.de"],
                )
            ],
        )
    elif request.param == "ldap_mock_server":
        if "MEX_LDAP_SEARCH_BASE" not in os.environ:
            pytest.skip("Ldap mock server not configured")
