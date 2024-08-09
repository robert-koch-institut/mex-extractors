import pytest
from faker import Faker

from mex.extractors.artificial.main import factories, faker, identities
from mex.extractors.settings import Settings


@pytest.fixture(autouse=True)
def reduce_output(settings: Settings) -> None:
    """Automatically reduce the chattiness for text fields and item count for tests."""
    settings.artificial.chattiness = 5
    settings.artificial.count = 15


@pytest.fixture(name="faker")
def init_faker(settings: Settings) -> Faker:
    """Return a fully configured faker instance."""
    Faker.seed(settings.artificial.seed)
    faker_instance = faker()
    identity_map = identities(faker_instance)
    return factories(faker_instance, identity_map)
