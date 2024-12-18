from unittest.mock import MagicMock

import pytest
from pytest import MonkeyPatch

from mex.extractors.open_data.connector import OpenDataConnector
from mex.extractors.open_data.models.source import (
    OpenDataCreatorsOrContributors,
    OpenDataLicense,
    OpenDataMetadata,
    OpenDataParentResource,
    OpenDataRelateditdentifiers,
    OpenDataResourceVersion,
)


@pytest.fixture
def mocked_open_data(monkeypatch: MonkeyPatch) -> None:
    """Mock the Open data connector to return dummy resources."""
    parent_resources = (
        OpenDataParentResource(conceptrecid="Eins", id=1),
        OpenDataParentResource(
            conceptrecid="Zwei",
            id=2,
            metadata=OpenDataMetadata(
                title="This is a test",
                description="<p> This is a test</p> <br>/n<a href> and </a>",
                creators=[OpenDataCreatorsOrContributors(name="Muster, Maxi")],
            ),
            conceptdoi="12.3456/zenodo.7890",
        ),
    )

    monkeypatch.setattr(
        OpenDataConnector, "get_parent_resources", lambda _: iter(parent_resources)
    )

    resource_versions = (
        OpenDataResourceVersion(conceptrecid="Eins", id=1),
        OpenDataResourceVersion(
            conceptrecid="Eins",
            id=2,
            metadata=OpenDataMetadata(
                license=OpenDataLicense(id="cc-by-4.0"),
                related_identifiers=[
                    OpenDataRelateditdentifiers(
                        identifier="should be transformed",
                        relation="isDocumentedBy",
                    ),
                    OpenDataRelateditdentifiers(
                        identifier="should be extracted but NOT transformed",
                        relation="isSupplementTo",
                    ),
                ],
            ),
        ),
    )

    monkeypatch.setattr(
        OpenDataConnector,
        "get_resource_versions",
        lambda self, _: iter(resource_versions),
    )

    def __init__(self: OpenDataConnector) -> None:
        self.session = MagicMock()
        self.url = "https://mock-opendata"

    monkeypatch.setattr(OpenDataConnector, "__init__", __init__)
