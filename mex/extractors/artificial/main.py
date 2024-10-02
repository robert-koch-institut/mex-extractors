from faker import Faker

from mex.common.cli import entrypoint
from mex.common.models import (
    EXTRACTED_MODEL_CLASSES,
    ExtractedAccessPlatform,
    ExtractedActivity,
    ExtractedContactPoint,
    ExtractedDistribution,
    ExtractedOrganization,
    ExtractedOrganizationalUnit,
    ExtractedPerson,
    ExtractedPrimarySource,
    ExtractedResource,
    ExtractedVariable,
    ExtractedVariableGroup,
)
from mex.extractors.artificial.identity import (
    IdentityMap,
    create_identities,
    restore_identities,
)
from mex.extractors.artificial.provider import (
    BuilderProvider,
    IdentityProvider,
    LinkProvider,
    PatternProvider,
    TemporalEntityProvider,
    TextProvider,
)
from mex.extractors.pipeline import asset, run_job_in_process
from mex.extractors.settings import Settings
from mex.extractors.sinks import load


@asset(group_name="artificial")
def faker() -> Faker:
    """Create and initialize a new faker instance."""
    settings = Settings.get()
    faker = Faker(settings.artificial.locale)
    faker.seed_instance(settings.artificial.seed)
    return faker


@asset(group_name="artificial")
def identities(faker: Faker) -> IdentityMap:
    """Create a map of identities for each of the weighted model classes."""
    return create_identities(
        faker,
        {
            ExtractedPrimarySource: 10,
            ExtractedAccessPlatform: 10,
            ExtractedActivity: 50,
            ExtractedContactPoint: 5,
            ExtractedDistribution: 50,
            ExtractedOrganization: 5,
            ExtractedOrganizationalUnit: 10,
            ExtractedPerson: 100,
            ExtractedResource: 70,
            ExtractedVariable: 500,
            ExtractedVariableGroup: 200,
        },
    )


@asset(group_name="artificial")
def factories(faker: Faker, identities: IdentityMap) -> Faker:
    """Create faker providers and register them on each factory."""
    for factory in faker.factories:
        factory.add_provider(IdentityProvider(factory, identities))
        factory.add_provider(LinkProvider(factory))
        factory.add_provider(PatternProvider(factory))
        factory.add_provider(BuilderProvider(factory))
        factory.add_provider(TextProvider(factory))
        factory.add_provider(TemporalEntityProvider(factory))
    return faker


@asset(group_name="artificial")
def artificial_data(factories: Faker, identities: IdentityMap) -> None:
    """Create artificial data and load the models to the sinks."""
    restore_identities(identities)  # restore state of memory identity provider
    for model in EXTRACTED_MODEL_CLASSES:
        load(factories.extracted_data(model))


@entrypoint(Settings)
def run() -> None:
    """Run the artificial data job in-process."""
    run_job_in_process("artificial")
