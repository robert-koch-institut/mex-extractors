from unittest.mock import MagicMock

import pytest

from mex.common.models import (
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedPerson,
    ExtractedPrimarySource,
    ResourceMapping,
)
from mex.common.testing import Joker
from mex.common.types import (
    Identifier,
    Link,
    LinkLanguage,
    TextLanguage,
    Theme,
)
from mex.extractors.biospecimen.models.source import BiospecimenResource
from mex.extractors.biospecimen.transform import (
    transform_biospecimen_resource_to_mex_resource,
)


@pytest.fixture
def synopse_extracted_activities() -> list[ExtractedActivity]:
    return [
        ExtractedActivity(
            hadPrimarySource="bVro4tpIg0kIjZubkhTmtE",
            identifierInPrimarySource="12345",
            contact=["c3oRNA1BYGvbwmrev070IL"],
            responsibleUnit=["bFQoRhcVH5DHUD"],
            title="Studie zu Lorem und Ipsum",
            abstract="BBCCDD-Basiserhebung am RKI.",
            activityType=["https://mex.rki.de/item/activity-type-6"],
            alternativeTitle=[],
            documentation=[
                Link(
                    language=None,
                    title="- Fragebogen\n- Labor",
                    url="file:///Z:/Projekte/Dokumentation",
                )
            ],
            end="2000",
            involvedPerson=["bFQoRhcVH5DHUC"],
            shortName="BBCCDD_00",
            start="1999",
            succeeds=["b9P7Ta27MTQLx7VijuZ2yX"],
            theme=Theme("https://mex.rki.de/item/theme-36"),
        ),
        ExtractedActivity(
            hadPrimarySource="bVro4tpIg0kIjZubkhTmtE",
            identifierInPrimarySource="1234567",
            contact=["c3oRNA1BYGvbwmrev070IL"],
            responsibleUnit=["bFQoRhcVH5DHUD"],
            title="Studie zu Lorem und Ipsum",
            abstract="BBCCDD-Basiserhebung am RKI.",
            activityType=["https://mex.rki.de/item/activity-type-6"],
            documentation=[
                Link(
                    language=None,
                    title="- Fragebogen\n- Labor",
                    url="file:///Z:/Projekte/Dokumentation",
                )
            ],
            end="2000",
            involvedPerson=["bFQoRhcVH5DHUC"],
            shortName="BBCCDD",
            start="1999",
            theme=[Theme("https://mex.rki.de/item/theme-36")],
        ),
    ]


def test_transform_biospecimen_resource_to_mex_resource(  # noqa: PLR0913
    extracted_primary_sources: dict[str, ExtractedPrimarySource],
    biospecimen_resources: list[BiospecimenResource],
    mex_persons: list[ExtractedPerson],
    extracted_organization_rki: ExtractedOrganization,
    synopse_extracted_activities: list[ExtractedActivity],
    resource_mapping: ResourceMapping,
) -> None:
    unit_stable_target_ids = MagicMock()
    unit_stable_target_ids.get.side_effect = lambda _: Identifier.generate(seed=42)

    synopse_merged_id = next(
        filter(
            lambda x: x.identifierInPrimarySource == "1234567",
            synopse_extracted_activities,
        )
    ).stableTargetId

    mex_sources = transform_biospecimen_resource_to_mex_resource(
        biospecimen_resources,
        extracted_primary_sources["biospecimen"],
        unit_stable_target_ids,
        mex_persons,
        extracted_organization_rki,
        synopse_extracted_activities,
        resource_mapping,
        {},
    )
    mex_source = next(mex_sources)

    expected = {
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "alternativeTitle": [{"value": "alternativer Testitel"}],
        "anonymizationPseudonymization": [
            "https://mex.rki.de/item/anonymization-pseudonymization-2"
        ],
        "conformsTo": [
            "LOINC",
        ],
        "contact": [str(mex_persons[0].stableTargetId)],
        "contributingUnit": [str(Identifier.generate(seed=42))],
        "contributor": [str(Identifier.generate(seed=42))],
        "description": [{"language": TextLanguage.DE, "value": "Testbeschreibung"}],
        "documentation": [
            {
                "language": LinkLanguage.DE,
                "title": "Testdokutitel",
                "url": "Testdokupfad",
            }
        ],
        "externalPartner": ["b0J5Ayp4XP3Yn8ta44Irhh"],
        "hadPrimarySource": str(
            extracted_primary_sources["biospecimen"].stableTargetId
        ),
        "hasLegalBasis": [
            {
                "language": TextLanguage.DE,
                "value": "DSGVO",
            },
        ],
        "hasPersonalData": "https://mex.rki.de/item/personal-data-1",
        "identifier": Joker(),
        "identifierInPrimarySource": "test_bioproben_Probe1",
        "instrumentToolOrApparatus": [{"value": "Testtool"}],
        "keyword": [
            {"language": TextLanguage.DE, "value": "Testschlagwort 1, Testschlagwort 2"}
        ],
        "language": [
            "https://mex.rki.de/item/language-1",
        ],
        "loincId": ["https://loinc.org/12345-6"],
        "meshId": ["http://id.nlm.nih.gov/mesh/D123"],
        "method": [{"language": TextLanguage.EN, "value": "Testmethode"}],
        "methodDescription": [
            {"language": TextLanguage.DE, "value": "Testmethodenbeschreibung"}
        ],
        "publisher": [str(extracted_organization_rki.stableTargetId)],
        "resourceCreationMethod": [
            "https://mex.rki.de/item/resource-creation-method-2",
            "https://mex.rki.de/item/resource-creation-method-4",
        ],
        "resourceTypeSpecific": [
            {"language": TextLanguage.DE, "value": "spezieller Testtyp"}
        ],
        "rights": [{"language": TextLanguage.DE, "value": "Testrechte"}],
        "sizeOfDataBasis": "Testanzahl",
        "spatial": [{"language": TextLanguage.DE, "value": "räumlicher Testbezug"}],
        "stableTargetId": Joker(),
        "temporal": "2021-09 bis 2021-10",
        "theme": [
            "https://mex.rki.de/item/theme-36",
        ],
        "title": [{"value": "test_titel"}],
        "unitInCharge": [str(Identifier.generate(seed=42))],
        "wasGeneratedBy": str(synopse_merged_id),
        # created_in_context_of is None, therefore not displayed
    }
    assert mex_source.model_dump(exclude_none=True, exclude_defaults=True) == expected
