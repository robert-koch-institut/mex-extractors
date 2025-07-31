import pytest

from mex.common.models import (
    MergedActivity,
    MergedBibliographicResource,
    MergedOrganization,
    MergedOrganizationalUnit,
    MergedPerson,
)
from mex.common.models.primary_source import PreviewPrimarySource
from mex.extractors.datenkompass.models.item import DatenkompassActivity
from tests.datenkompass.mocked_backend_api_connector import (
    create_mocked_bmg,
    create_mocked_datenkompass_activity,
    create_mocked_merged_activities,
    create_mocked_merged_bibliographic_resource,
    create_mocked_merged_organizational_units,
    create_mocked_merged_person,
    create_mocked_preview_primary_sources,
)


@pytest.fixture
def mocked_merged_activities() -> list[MergedActivity]:
    """Mock a list of Merged Activity items."""
    return create_mocked_merged_activities()


@pytest.fixture
def mocked_merged_bibliographic_resource() -> list[MergedBibliographicResource]:
    """Mock a list of Merged Bibliographic Resource items."""
    return create_mocked_merged_bibliographic_resource()


@pytest.fixture
def mocked_merged_organizational_units() -> list[MergedOrganizationalUnit]:
    """Mock a list of Merged Organizational Unit items."""
    return create_mocked_merged_organizational_units()


@pytest.fixture
def mocked_bmg() -> list[MergedOrganization]:
    """Mock a list of BMG as Merged Organization items."""
    return create_mocked_bmg()


@pytest.fixture
def mocked_merged_person() -> list[MergedPerson]:
    """Mock a single Merged Person item."""
    return create_mocked_merged_person()


@pytest.fixture
def mocked_preview_primary_sources() -> list[PreviewPrimarySource]:
    """Mock a list of Preview Primary Source items."""
    return create_mocked_preview_primary_sources()


@pytest.fixture
def mocked_datenkompass_activity() -> list[DatenkompassActivity]:
    """Mock a list of Datenkompass Activity items."""
    return create_mocked_datenkompass_activity()
