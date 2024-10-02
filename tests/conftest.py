import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, Mock
from uuid import UUID

import pytest
import requests
from pytest import MonkeyPatch
from requests import Response

from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models.actor import LDAPActor
from mex.common.ldap.models.person import LDAPPerson
from mex.common.models import ExtractedOrganization
from mex.common.types import MergedPrimarySourceIdentifier
from mex.common.wikidata.connector import (
    WikidataAPIConnector,
    WikidataQueryServiceConnector,
)
from mex.common.wikidata.models.organization import WikidataOrganization
from mex.extractors.settings import Settings

pytest_plugins = (
    "mex.common.testing.plugin",
    "tests.blueant.mocked_blueant",
    "tests.confluence_vvt.mocked_confluence_vvt",
    "tests.datscha_web.mocked_datscha_web",
    "tests.grippeweb.mocked_grippeweb",
    "tests.ifsg.mocked_ifsg",
    "tests.rdmo.mocked_rdmo",
    "tests.drop.mocked_drop",
)

TEST_DATA_DIR = Path(__file__).parent / "test_data"


@pytest.fixture(autouse=True)
def settings() -> Settings:
    """Load the settings for this pytest session."""
    return Settings.get()


@pytest.fixture
def wikidata_organization_raw() -> dict[str, Any]:
    """Return a raw wikidata organization."""
    with open(TEST_DATA_DIR / "wikidata_organization_raw.json") as fh:
        return json.load(fh)


@pytest.fixture
def wikidata_organization(
    wikidata_organization_raw: dict[str, Any],
) -> WikidataOrganization:
    """Return a wikidata organization instance."""
    return WikidataOrganization.model_validate(wikidata_organization_raw)


@pytest.fixture
def extracted_organization_rki() -> ExtractedOrganization:
    return ExtractedOrganization(
        identifierInPrimarySource="Robert Koch-Institut",
        hadPrimarySource=MergedPrimarySourceIdentifier.generate(123),
        officialName=["Robert Koch-Institut"],
    )


@pytest.fixture
def mocked_wikidata(
    monkeypatch: MonkeyPatch, wikidata_organization_raw: dict[str, Any]
) -> None:
    """Mock wikidata connector."""
    response_query = Mock(spec=Response, status_code=200)

    session = MagicMock(spec=requests.Session)
    session.get = MagicMock(side_effect=[response_query])

    def mocked_init(self: WikidataQueryServiceConnector) -> None:
        self.session = session

    monkeypatch.setattr(WikidataQueryServiceConnector, "__init__", mocked_init)
    monkeypatch.setattr(WikidataAPIConnector, "__init__", mocked_init)

    # mock search_wikidata_with_query

    def get_data_by_query(
        self: WikidataQueryServiceConnector, query: str
    ) -> list[dict[str, dict[str, str]]]:
        return [
            {
                "item": {
                    "type": "uri",
                    "value": "http://www.wikidata.org/entity/Q26678",
                },
                "itemLabel": {"xml:lang": "en", "type": "literal", "value": "BMW"},
                "itemDescription": {
                    "xml:lang": "en",
                    "type": "literal",
                    "value": "German automotive manufacturer, and conglomerate",
                },
            },
        ]

    monkeypatch.setattr(
        WikidataQueryServiceConnector, "get_data_by_query", get_data_by_query
    )

    # mock get_wikidata_org_with_org_id

    def get_wikidata_item_details_by_id(
        self: WikidataQueryServiceConnector, item_id: str
    ) -> dict[str, str]:
        return wikidata_organization_raw

    monkeypatch.setattr(
        WikidataAPIConnector,
        "get_wikidata_item_details_by_id",
        get_wikidata_item_details_by_id,
    )


@pytest.fixture
def mocked_ldap(monkeypatch: MonkeyPatch) -> None:
    """Mock the LDAP connector to return resolved persons and units."""
    actors = [
        LDAPActor(
            sAMAccountName="ContactC",
            objectGUID=UUID(int=4, version=4),
            mail=["email@email.de", "contactc@rki.de"],
        )
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
