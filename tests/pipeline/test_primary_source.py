from mex.common.models import (
    MEX_PRIMARY_SOURCE_IDENTIFIER,
    MEX_PRIMARY_SOURCE_IDENTIFIER_IN_PRIMARY_SOURCE,
    MEX_PRIMARY_SOURCE_STABLE_TARGET_ID,
)
from mex.extractors.pipeline.primary_source import (
    extracted_primary_source_ldap,
    extracted_primary_source_mex,
    extracted_primary_source_organigram,
    extracted_primary_source_wikidata,
    extracted_primary_sources,
)


def test_extracted_primary_sources() -> None:
    primary_sources = extracted_primary_sources()
    assert len(primary_sources) > 10
    assert primary_sources[0].model_dump(exclude_defaults=True) == {
        "identifier": str(MEX_PRIMARY_SOURCE_IDENTIFIER),
        "hadPrimarySource": str(MEX_PRIMARY_SOURCE_STABLE_TARGET_ID),
        "identifierInPrimarySource": str(
            MEX_PRIMARY_SOURCE_IDENTIFIER_IN_PRIMARY_SOURCE
        ),
        "stableTargetId": str(MEX_PRIMARY_SOURCE_STABLE_TARGET_ID),
    }


def test_extracted_primary_source_mex() -> None:
    primary_sources = extracted_primary_sources()
    extracted_primary_source = extracted_primary_source_mex(primary_sources)
    assert extracted_primary_source.identifierInPrimarySource == "mex"


def test_extracted_primary_source_ldap() -> None:
    primary_sources = extracted_primary_sources()
    extracted_primary_source = extracted_primary_source_ldap(primary_sources)
    assert extracted_primary_source.identifierInPrimarySource == "ldap"


def test_extracted_primary_source_organigram() -> None:
    primary_sources = extracted_primary_sources()
    extracted_primary_source = extracted_primary_source_organigram(primary_sources)
    assert extracted_primary_source.identifierInPrimarySource == "organigram"


def test_extracted_primary_source_wikidata() -> None:
    primary_sources = extracted_primary_sources()
    extracted_primary_source = extracted_primary_source_wikidata(primary_sources)
    assert extracted_primary_source.identifierInPrimarySource == "wikidata"
