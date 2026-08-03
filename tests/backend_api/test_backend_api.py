import pytest

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.models import MEX_PRIMARY_SOURCE_STABLE_TARGET_ID, ExtractedContactPoint
from mex.common.sinks.backend_api import BackendApiSink

pytestmark = [pytest.mark.integration, pytest.mark.xdist_group(name="backend_api")]


def test_backend_api_is_available() -> None:
    connector = BackendApiConnector.get()  # already health-checks on construction
    status = connector.system_status()
    assert status.status == "ok"


def test_identity_assign_is_idempotent() -> None:
    # use the connector directly, bypassing BackendApiIdentityProvider's
    # client-side lru_cache, so both assigns actually hit the backend
    identifier_in_primary_source = "extractors-identity-roundtrip"
    connector = BackendApiConnector.get()

    first = connector.assign_identity(
        had_primary_source=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
        identifier_in_primary_source=identifier_in_primary_source,
    )
    second = connector.assign_identity(
        had_primary_source=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
        identifier_in_primary_source=identifier_in_primary_source,
    )

    assert first == second


def test_ingest_roundtrip() -> None:
    identifier_in_primary_source = "extractors-ingest-roundtrip"
    contact_point = ExtractedContactPoint(
        hadPrimarySource=MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
        identifierInPrimarySource=identifier_in_primary_source,
        email=["test@example.com"],
    )

    sink = BackendApiSink.get()
    list(sink.load([contact_point]))

    connector = BackendApiConnector.get()
    result = connector.fetch_extracted_items(
        stable_target_id=str(contact_point.stableTargetId),
    )
    assert result.total == 1
    assert result.items[0].identifierInPrimarySource == identifier_in_primary_source
