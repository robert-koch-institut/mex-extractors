import pytest
from pytest import MonkeyPatch

import mex.extractors.datenkompass.extract as extract_module
from mex.common.identity import Identity


@pytest.fixture
def mocked_provider(
    monkeypatch: MonkeyPatch,
) -> None:
    """Mock the IdentityProvider to return dummy variables."""

    class FakeProvider:
        def fetch(self, stable_target_id: str) -> list[Identity]:
            if stable_target_id == "SomeIrrelevantPS":
                return [
                    Identity(
                        identifier="12345678901234",
                        hadPrimarySource="00000000000000",
                        identifierInPrimarySource="completely irrelevant",
                        stableTargetId="SomeIrrelevantPS",
                    )
                ]
            if stable_target_id == "identifierRelevantPS":
                return [
                    Identity(
                        identifier="98765432109876",
                        hadPrimarySource="00000000000000",
                        identifierInPrimarySource="relevant primary source",
                        stableTargetId="identifierRelevantPS",
                    )
                ]
            pytest.fail("wrong mocking of identity provider")

    def fake_get_provider() -> FakeProvider:
        return FakeProvider()

    monkeypatch.setattr(extract_module, "get_provider", fake_get_provider)
