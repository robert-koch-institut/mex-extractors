from binascii import crc32

from faker import Faker

from mex.common.identity import Identity, get_provider
from mex.common.identity.memory import MemoryIdentityProvider
from mex.common.models import (
    EXTRACTED_MODEL_CLASSES,
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
)
from mex.common.types import MergedPrimarySourceIdentifier
from mex.extractors.settings import Settings

IdentityMap = dict[str, list[Identity]]


def restore_identities(identity_map: IdentityMap) -> None:
    """Restore the state of the memory identity provider.

    Because identity creation and model instantiation happen in different subprocesses,
    the identity provider does not have access to previously stored identities.

    Args:
        identity_map: Identity map that needs to be loaded back into the provider
    """
    # TODO(ND): Resolve this ugliness by letting the MemoryIdentityProvider cache its
    # identity map in the dagster io manager.
    identity_provider = get_provider()
    if isinstance(identity_provider, MemoryIdentityProvider):
        for identities in identity_map.values():
            identity_provider._database.extend(identities)  # noqa: SLF001
    # if the identity_provider is not `memory`, we don't need to restore its state
    # because it should persist its state in its own database anyway


def _get_offset_int(cls: type) -> int:
    """Calculate an integer based on the crc32 checksum of the name of a class."""
    return crc32(cls.__name__.encode())


def _create_numeric_ids(faker: Faker) -> dict[str, list[int]]:
    """Create a mapping from entity type to a list of numeric ids.

    These numeric ids can be used as seeds for the identity of artificial items.
    The seeds will be passed to `Identifier.generate(seed=...)` to get deterministic
    identifiers throughout consecutive runs of the artificial extractor.

    Args:
        faker: Instance of faker

    Returns:
        Dict with entity types and lists of numeric ids
    """
    settings = Settings.get()
    # pick a random selection of model classes from list of extracted model classes
    choices = list(
        faker.random_choices(
            EXTRACTED_MODEL_CLASSES,
            length=settings.artificial.count - len(EXTRACTED_MODEL_CLASSES),
        )
    )
    # count the picks, but use at least 2 so we can fulfill required references
    counts = {model: max(2, choices.count(model)) for model in EXTRACTED_MODEL_CLASSES}
    # build a static offset integer per class to spread out the id ranges
    offsets = {model: _get_offset_int(model) for model in EXTRACTED_MODEL_CLASSES}
    # calculate numeric ids per model in the calculated quantities
    return {
        model.__name__.removeprefix("Extracted"): list(
            range(offsets[model], offsets[model] + counts[model])
        )
        for model in EXTRACTED_MODEL_CLASSES
    }


def create_identities(faker: Faker) -> IdentityMap:
    """Create the identities of the to-be-faked models.

    We do this **before** actually creating the models, because we need to be able
    to set existing stableTargetIds on reference fields.

    Args:
        faker: Instance of faker

    Returns:
        Dict with entity types and lists of Identities
    """
    # create the numeric id dictionary
    numeric_ids = _create_numeric_ids(faker)
    # store a map of identities by entity type
    identity_map: IdentityMap = {entity_type: [] for entity_type in numeric_ids}
    # primary sources need identities first
    identity_provider = get_provider()
    for entity_type in sorted(identity_map, key=lambda t: t != "PrimarySource"):
        for numeric_id in numeric_ids[entity_type]:
            # create a persistent identity for each numeric id
            if primary_sources := identity_map["PrimarySource"]:
                # pick a random primary source to "extract" the identity from
                primary_source: Identity = faker.random_element(primary_sources)
                primary_source_id = MergedPrimarySourceIdentifier(
                    primary_source.stableTargetId
                )
            else:
                # the first primary source is extracted from this placeholder ID
                primary_source_id = MEX_PRIMARY_SOURCE_STABLE_TARGET_ID
            identity = identity_provider.assign(
                primary_source_id, f"{entity_type}-{numeric_id}"
            )
            identity_map[entity_type].append(identity)
    return identity_map
