
import pytest

from mex.common.ldap.models import LDAPActor
from mex.common.models import (
    AccessPlatformMapping,
    ExtractedContactPoint,
    ResourceMapping,
)
from mex.common.types import MergedPrimarySourceIdentifier
from mex.extractors.igs.extract import extract_igs_schemas
from mex.extractors.igs.model import IGSSchema
from mex.extractors.settings import Settings
from mex.extractors.utils import load_yaml


@pytest.fixture
def igs_access_platform_mapping() -> AccessPlatformMapping:
    settings = Settings.get()
    return AccessPlatformMapping.model_validate(
        load_yaml(settings.igs.mapping_path / "access-platform.yaml")
    )


@pytest.fixture
def igs_resource_mapping() -> ResourceMapping:
    settings = Settings.get()
    return ResourceMapping.model_validate(
        load_yaml(settings.igs.mapping_path / "resource.yaml")
    )


@pytest.fixture
def extracted_igs_contact_points_by_mail() -> dict[str, ExtractedContactPoint]:
    """Mock IGS actor."""
    return {
        "email@email.de":
        ExtractedContactPoint(
            email="email@email.de",
            hadPrimarySource=MergedPrimarySourceIdentifier.generate(seed=42),
            identifierInPrimarySource="actor 1",
        ),
        "contactc@rki.de":
        ExtractedContactPoint(
            email="contactc@rki.de",
            hadPrimarySource=MergedPrimarySourceIdentifier.generate(seed=43),
            identifierInPrimarySource="actor 2",
        )
        }


@pytest.fixture
def igs_schemas() -> dict[str, IGSSchema]:
    return extract_igs_schemas()
