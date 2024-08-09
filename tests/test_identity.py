from unittest.mock import MagicMock, Mock

import pytest
import requests
from pytest import MonkeyPatch
from requests import HTTPError

from mex.common.identity import Identity
from mex.common.models import (
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
    ExtractedContactPoint,
    ExtractedPrimarySource,
)
from mex.common.testing import Joker
from mex.common.types import MergedPrimarySourceIdentifier
from mex.extractors.identity import BackendIdentityProvider
from mex.extractors.sinks import load


@pytest.fixture
def mocked_backend_identity_provider(monkeypatch: MonkeyPatch) -> MagicMock:
    mocked_session = MagicMock(spec=requests.Session)
    mocked_session.request = MagicMock(
        return_value=Mock(spec=requests.Response, status_code=200)
    )
    mocked_session.headers = {}

    def set_mocked_session(self: BackendIdentityProvider) -> None:
        self.session = mocked_session

    monkeypatch.setattr(BackendIdentityProvider, "_set_session", set_mocked_session)
    return mocked_session


def test_assign_mocked(
    mocked_backend_identity_provider: requests.Session,
) -> None:
    mocked_data = {
        "identifier": MergedPrimarySourceIdentifier.generate(seed=962),
        "hadPrimarySource": MergedPrimarySourceIdentifier.generate(seed=961),
        "identifierInPrimarySource": "test",
        "stableTargetId": MergedPrimarySourceIdentifier.generate(seed=963),
    }
    mocked_response = Mock(spec=requests.Response)
    mocked_response.status_code = 200
    mocked_response.json = MagicMock(return_value=mocked_data)
    mocked_backend_identity_provider.request = MagicMock(return_value=mocked_response)

    provider = BackendIdentityProvider.get()
    identity_first = provider.assign(
        had_primary_source=MergedPrimarySourceIdentifier.generate(seed=961),
        identifier_in_primary_source="test",
    )

    identity = Identity.model_validate(identity_first)

    identity_first_assignment = identity.model_dump()

    assert identity_first_assignment == mocked_data

    identity_second = provider.assign(
        had_primary_source=MergedPrimarySourceIdentifier.generate(seed=961),
        identifier_in_primary_source="test",
    )
    identity_second_assignment = identity_second.model_dump()

    assert identity_second_assignment == identity_first_assignment


def test_fetch_mocked(
    mocked_backend_identity_provider: requests.Session,
) -> None:
    mocked_data = {
        "items": [
            {
                "identifier": MergedPrimarySourceIdentifier.generate(seed=962),
                "hadPrimarySource": MergedPrimarySourceIdentifier.generate(seed=961),
                "identifierInPrimarySource": "test",
                "stableTargetId": MergedPrimarySourceIdentifier.generate(seed=963),
            }
        ],
        "total": 1,
    }

    mocked_response = Mock(spec=requests.Response)
    mocked_response.status_code = 200
    mocked_response.json = MagicMock(return_value=mocked_data)
    mocked_backend_identity_provider.request = MagicMock(return_value=mocked_response)

    provider = BackendIdentityProvider.get()

    contact_point = ExtractedContactPoint(
        hadPrimarySource=MergedPrimarySourceIdentifier.generate(seed=961),
        identifierInPrimarySource="test",
        email=["test@test.de"],
    )

    identities = provider.fetch(stable_target_id=contact_point.stableTargetId)
    assert identities == [
        Identity(
            stableTargetId=mocked_data["items"][0]["stableTargetId"],
            identifier=mocked_data["items"][0]["identifier"],
            hadPrimarySource=contact_point.hadPrimarySource,
            identifierInPrimarySource=contact_point.identifierInPrimarySource,
        )
    ]

    identities = provider.fetch(
        had_primary_source=contact_point.hadPrimarySource,
        identifier_in_primary_source=contact_point.identifierInPrimarySource,
    )
    assert identities == [
        Identity(
            stableTargetId=mocked_data["items"][0]["stableTargetId"],
            identifier=mocked_data["items"][0]["identifier"],
            hadPrimarySource=contact_point.hadPrimarySource,
            identifierInPrimarySource=contact_point.identifierInPrimarySource,
        )
    ]


@pytest.mark.skip("Backend might not be running. To be fixed in MX-1523")
@pytest.mark.integration
def test_availability() -> None:
    provider = BackendIdentityProvider.get()
    try:
        provider._check_availability()
    except HTTPError:
        pytest.fail("HTTPError : Backend connection failed")


@pytest.mark.skip("Backend might not be running. To be fixed in MX-1523")
@pytest.mark.integration
def test_assign_identity() -> None:
    provider = BackendIdentityProvider.get()
    provider.assign.cache_clear()
    identity_first = provider.assign(
        had_primary_source=MergedPrimarySourceIdentifier.generate(seed=961),
        identifier_in_primary_source="test",
    )
    identity_first_assignment = identity_first.model_dump()

    assert identity_first_assignment == {
        "hadPrimarySource": MergedPrimarySourceIdentifier.generate(seed=961),
        "identifierInPrimarySource": "test",
        "stableTargetId": Joker(),
        "identifier": Joker(),
    }

    identity_second = provider.assign(
        had_primary_source=MergedPrimarySourceIdentifier.generate(seed=961),
        identifier_in_primary_source="test",
    )
    identity_second_assignment = identity_second.model_dump()

    assert identity_second_assignment == identity_first_assignment

    cached_info = provider.assign.cache_info()

    assert cached_info.hits == 1
    assert cached_info.misses == 1


@pytest.mark.skip("Backend might not be running. To be fixed in MX-1523")
@pytest.mark.integration
def test_fetch_identity() -> None:
    provider = BackendIdentityProvider.get()

    primary_source = ExtractedPrimarySource(
        hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
        identifierInPrimarySource="test",
    )
    load([primary_source])  # this needs to be loaded in Sink.BACKEND

    contact_point = ExtractedContactPoint(
        hadPrimarySource=primary_source.stableTargetId,
        identifierInPrimarySource="test",
        email=["test@test.de"],
    )

    identities = provider.fetch(
        had_primary_source=contact_point.hadPrimarySource,
        identifier_in_primary_source=contact_point.identifierInPrimarySource,
    )
    assert identities == []

    identities = provider.fetch(stable_target_id=contact_point.stableTargetId)
    assert identities == []

    load([contact_point])  # this needs to be loaded in Sink.BACKEND

    identities = provider.fetch(stable_target_id=contact_point.stableTargetId)
    assert identities == [
        Identity(
            stableTargetId=contact_point.stableTargetId,
            identifier=contact_point.identifier,
            hadPrimarySource=contact_point.hadPrimarySource,
            identifierInPrimarySource=contact_point.identifierInPrimarySource,
        )
    ]

    identities = provider.fetch(
        had_primary_source=contact_point.hadPrimarySource,
        identifier_in_primary_source=contact_point.identifierInPrimarySource,
    )

    assert identities == [
        Identity(
            stableTargetId=contact_point.stableTargetId,
            identifier=contact_point.identifier,
            hadPrimarySource=contact_point.hadPrimarySource,
            identifierInPrimarySource=contact_point.identifierInPrimarySource,
        )
    ]
