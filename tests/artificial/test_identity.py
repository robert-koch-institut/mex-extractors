from itertools import pairwise
from typing import TYPE_CHECKING, cast

from faker import Faker

from mex.common.identity import get_provider
from mex.common.models import EXTRACTED_MODEL_CLASSES
from mex.common.testing import Joker
from mex.common.types import MergedPrimarySourceIdentifier
from mex.extractors.artificial.identity import (
    _create_numeric_ids,
    _get_offset_int,
    create_identities,
    restore_identities,
)
from mex.extractors.settings import Settings

if TYPE_CHECKING:
    from mex.common.identity.memory import MemoryIdentityProvider


def test_restore_identities() -> None:
    # assign a new dummy identity
    identity_provider = get_provider()
    dummy_identity = identity_provider.assign(
        MergedPrimarySourceIdentifier.generate(), "dummy"
    )

    # trash the current provider (simulating what happens between dagster assets)
    cast("MemoryIdentityProvider", identity_provider)._database.clear()

    # try to restore the identity map
    restore_identities({"dummy": [dummy_identity]})

    # fetch the restored identity
    identity_provider = get_provider()
    restored_identity = identity_provider.fetch(
        had_primary_source=dummy_identity.hadPrimarySource,
        identifier_in_primary_source=dummy_identity.identifierInPrimarySource,
    )[0]

    # ensure they are the same
    assert dummy_identity == restored_identity


def test_create_numeric_ids(faker: Faker) -> None:
    numeric_ids = _create_numeric_ids(faker)
    assert numeric_ids == {
        "AccessPlatform": [3427526317, 3427526318],
        "Activity": [2405477151, 2405477152],
        "BibliographicResource": [2035724763, 2035724764],
        "Consent": [2933388007, 2933388008],
        "ContactPoint": [4181830114, 4181830115],
        "Distribution": [1589096615, 1589096616],
        "Organization": [991028314, 991028315],
        "OrganizationalUnit": [3284134756, 3284134757],
        "Person": [639677827, 639677828],
        "PrimarySource": [2516530558, 2516530559],
        "Resource": [2676315731, 2676315732],
        "Variable": [4015597000, 4015597001],
        "VariableGroup": [1476619723, 1476619724],
    }


def test_get_offset_int() -> None:
    offsets = [_get_offset_int(model) for model in EXTRACTED_MODEL_CLASSES]
    assert offsets[0] == 3427526317  # sanity check

    # ensure the distance between the offsets is greater than the allowed total
    min_distance = min([abs(j - i) for i, j in pairwise(offsets)])
    max_allowed = (
        Settings.model_fields["artificial"]
        .default.model_fields["count"]
        .metadata[-1]
        .lt
    )
    assert min_distance > max_allowed


def test_create_identities(faker: Faker) -> None:
    identity_map = create_identities(faker)
    assert len(identity_map) == len(EXTRACTED_MODEL_CLASSES)
    assert identity_map["PrimarySource"][0].model_dump() == {
        "identifier": Joker(),
        "hadPrimarySource": "00000000000000",
        "identifierInPrimarySource": "PrimarySource-2516530558",
        "stableTargetId": Joker(),
    }
