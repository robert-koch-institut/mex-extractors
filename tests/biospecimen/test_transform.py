from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import pytest

from mex.common.models import (
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedPerson,
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
from mex.extractors.biospecimen.transform import (
    transform_biospecimen_resource_to_mex_resource,
)
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)

if TYPE_CHECKING:
    from mex.extractors.biospecimen.models.source import BiospecimenResource


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


@pytest.mark.usefixtures("mocked_wikidata")
def test_transform_biospecimen_resource_to_mex_resource(  # noqa: PLR0913
    biospecimen_resources: list[BiospecimenResource],
    roland_resolved: ExtractedPerson,
    juturna_felicitas: ExtractedPerson,
    frieda_fictitious: ExtractedPerson,
    extracted_organization_rki: ExtractedOrganization,
    synopse_extracted_activities: list[ExtractedActivity],
    resource_mapping: ResourceMapping,
) -> None:
    unit_stable_target_ids = MagicMock()
    unit_stable_target_ids.get.side_effect = lambda key, default=None: [
        Identifier.generate(seed=42)
    ]

    synopse_merged_id = next(
        filter(
            lambda x: x.identifierInPrimarySource == "1234567",
            synopse_extracted_activities,
        )
    ).stableTargetId

    resources = transform_biospecimen_resource_to_mex_resource(
        biospecimen_resources,
        [roland_resolved, juturna_felicitas, frieda_fictitious],
        extracted_organization_rki,
        synopse_extracted_activities,
        resource_mapping,
        {},
    )

    expected = {
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "alternativeTitle": [{"value": "alternativer Testitel"}],
        "anonymizationPseudonymization": [
            "https://mex.rki.de/item/anonymization-pseudonymization-2"
        ],
        "conformsTo": [
            "LOINC",
        ],
        "contact": [str(roland_resolved.stableTargetId)],
        "description": [{"language": TextLanguage.DE, "value": "Testbeschreibung"}],
        "documentation": [
            {
                "language": LinkLanguage.DE,
                "title": "Testdokutitel",
                "url": "Testdokupfad",
            }
        ],
        "externalPartner": ["b0J5Ayp4XP3Yn8ta44Irhh"],
        "hadPrimarySource": str(get_extracted_primary_source_id_by_name("biospecimen")),
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
        "unitInCharge": ["hIiJpZXVppHvoyeP0QtAoS"],
        "wasGeneratedBy": str(synopse_merged_id),
        # created_in_context_of is None, therefore not displayed
    }
    assert resources[0].model_dump(exclude_none=True, exclude_defaults=True) == expected
