from collections.abc import Generator

import pytest

from mex.common.backend_api import connector
from mex.common.models import (
    AnyMergedModel,
    MergedConsent,
    MergedPerson,
)
from mex.common.types import (
    TemporalEntity,
)


@pytest.fixture
def mocked_consent_backend_api_connector(
    monkeypatch: pytest.MonkeyPatch,
) -> dict[str, int]:
    """Mock the backendAPIConnector to return dummy variables."""
    person_store = [
        MergedPerson(
            identifier="efVXbcoxxMAiA5IqT1d11H",
            email=[
                "consent_private_mail@mail-provider.abc",
                "consent_working_mail@rki.de",
            ],
            fullName=["Consent Person"],
        ),
        MergedPerson(
            identifier="efVXbcoxxMAiA5IqT1d22H",
            email=[
                "NOconsent_private_mail@mail-provider.abc",
                "NOconsent_working_mail@rki.de",
            ],
            fullName=["NO Consent Person"],
        ),
    ]
    consent_store = [
        MergedConsent(
            hasDataSubject="efVXbcoxxMAiA5IqT1d11H",
            isIndicatedAtTime=TemporalEntity("2025-07-31T10:18:35Z"),
            hasConsentStatus="https://mex.rki.de/item/consent-status-1",
            identifier="efVXbcoxxMAiA5IqT1d10C",
        )
    ]

    call_counter = {"count": 0}

    class FakeConnector:
    ...
    ) -> Generator[AnyMergedModel, None, None]:
        call_counter["count"] += 1
        if (
            entity_type
            and "MergedPerson" in entity_type
            and reference_field == "hadPrimarySource"
        ):
        ...
    ...
            self,
            _: str | None = None,
            __: str | None = None,
            entity_type: list[str] | None = None,
            referenced_identifier: list[str] | None = None,
            reference_field: str | None = None,
        ) -> Generator[AnyMergedModel, None, None]:
            if (
                entity_type
                and "MergedPerson" in entity_type
                and reference_field == "hadPrimarySource"
            ):
                call_counter["count"] += 1
                return (x for x in person_store)

            if (
                entity_type
                and referenced_identifier
                and "MergedConsent" in entity_type
                and reference_field == "hasDataSubject"
            ):
                consents = list(
                    filter(
                        lambda x: x.hasDataSubject in referenced_identifier,
                        consent_store,
                    )
                )
                call_counter["count"] += 1
                return (x for x in consents)

            empty: list[AnyMergedModel] = []
            call_counter["count"] += 1
            return (x for x in empty)

    monkeypatch.setattr(connector.BackendApiConnector, "get", lambda: FakeConnector())
    return call_counter
