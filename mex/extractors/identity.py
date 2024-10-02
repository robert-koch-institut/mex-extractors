from functools import cache
from urllib.parse import urljoin

from mex.common.connector.http import HTTPConnector
from mex.common.identity.base import BaseProvider
from mex.common.identity.models import Identity
from mex.common.types import Identifier, MergedPrimarySourceIdentifier
from mex.extractors.settings import Settings


class BackendIdentityProvider(BaseProvider, HTTPConnector):
    """Identity provider that communicates with the backend HTTP API."""

    API_VERSION = "v0"

    def _check_availability(self) -> None:
        """Send a GET request to verify the API is available."""
        self.request("GET", "_system/check")

    def _set_authentication(self) -> None:
        """Set the backend API key to all session headers."""
        settings = Settings.get()
        self.session.headers["X-API-Key"] = settings.backend_api_key.get_secret_value()

    def _set_url(self) -> None:
        """Set the backend api url with the version path."""
        settings = Settings.get()
        self.url = urljoin(str(settings.backend_api_url), self.API_VERSION)

    @cache  # noqa: B019
    def assign(
        self,
        had_primary_source: MergedPrimarySourceIdentifier,
        identifier_in_primary_source: str,
    ) -> Identity:
        """Find an Identity in a database or assign a new one."""
        payload = {
            "hadPrimarySource": had_primary_source,
            "identifierInPrimarySource": identifier_in_primary_source,
        }
        identity = self.request("POST", "identity", payload)

        return Identity.model_validate(identity)

    def fetch(
        self,
        *,
        had_primary_source: Identifier | None = None,
        identifier_in_primary_source: str | None = None,
        stable_target_id: Identifier | None = None,
    ) -> list[Identity]:
        """Find Identity instances matching the given filters.

        Either provide `stableTargetId` or `hadPrimarySource`
        and `identifierInPrimarySource` together to get a unique result.
        """
        params = {
            "hadPrimarySource": had_primary_source,
            "identifierInPrimarySource": identifier_in_primary_source,
            "stableTargetId": stable_target_id,
        }
        params_cleaned = {key: str(value) for key, value in params.items() if value}
        results = self.request("GET", "identity", params=params_cleaned)
        return [Identity.model_validate(i) for i in results["items"]]
