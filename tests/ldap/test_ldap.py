import pytest

from mex.common.ldap.connector import LDAPConnector

from typing import Any, cast
from urllib.parse import urlsplit

import backoff, re
from ldap3 import AUTO_BIND_NO_TLS, Connection, Server
from ldap3.core.exceptions import LDAPExceptionError, LDAPSocketSendError

from mex.common.connector import BaseConnector
from mex.common.exceptions import (
    EmptySearchResultError,
    FoundMoreThanOneError,
    MExError,
)
from mex.common.ldap.models import (
    LDAP_MODEL_CLASSES,
    AnyLDAPActor,
    AnyLDAPActorsTypeAdapter,
    LDAPFunctionalAccount,
    LDAPFunctionalAccountsTypeAdapter,
    LDAPPerson,
    LDAPPersonsTypeAdapter,
)
from mex.common.logging import logger
from mex.common.settings import BaseSettings

from mex.extractors.settings import Settings


@pytest.mark.integration
def test_mock_available() -> None:
    settings = Settings.get()
    url = urlsplit(settings.ldap_url.get_secret_value())
    host = str(url.hostname)
    port = int(url.port) if url.port else None
    server = Server(host, port, use_ssl=True)
    connection = Connection(
        server,
        user=url.username,
        password=url.password,
        #auto_bind=AUTO_BIND_NO_TLS,
        read_only=True,
    )
    connection.__enter__()
    try:
        connection.server.check_availability()
    except LDAPExceptionError as error:
        msg = f"LDAP service not available at url: {host}:{port}"
        raise MExError(msg) from error

    ldap_connector = LDAPConnector.get()
    assert ldap_connector.get_persons(limit=42)
    assert not ldap_connector.get_persons(limit=42)
