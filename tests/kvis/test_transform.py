import pytest

from mex.common.types import LinkLanguage, TextLanguage
from mex.extractors.kvis.transform import transform_kvis_resource_to_extracted_resource


@pytest.mark.usefixtures("mocked_wikidata")
def test_transform_kvis_resource_to_extracted_resource() -> None:
    extracted_resource = transform_kvis_resource_to_extracted_resource()
    assert extracted_resource.model_dump(
        exclude_defaults=True, exclude_none=True, exclude_computed_fields=True
    ) == {
        "accessRestriction": "https://mex.rki.de/item/access-restriction-2",
        "alternativeTitle": [{"language": TextLanguage.DE, "value": "KVIS"}],
        "contact": ["bFQoRhcVH5DIek"],
        "contributingUnit": ["6rqNvZSApUHlz8GkkVP48"],
        "contributor": ["bFQoRhcVH5DIek", "bFQoRhcVH5DIek"],
        "created": "1999",
        "description": [{"language": TextLanguage.DE, "value": "Wörter"}],
        "documentation": [
            {"language": LinkLanguage.DE, "title": "a", "url": "http://www.a.b"},
            {"language": LinkLanguage.DE, "title": "c", "url": "http://www.c.d"},
        ],
        "externalPartner": ["ga6xh6pgMwgq7DC7r6Wjqg"],
        "hadPrimarySource": "eKx0G7GVS8o9v537kCUM3i",
        "identifierInPrimarySource": "kvis_resource",
        "keyword": [{"language": TextLanguage.DE, "value": "Schlüsselwort"}],
        "language": ["https://mex.rki.de/item/language-1"],
        "publisher": ["ga6xh6pgMwgq7DC7r6Wjqg"],
        "theme": ["https://mex.rki.de/item/theme-11"],
        "title": [{"language": TextLanguage.DE, "value": "Titel"}],
        "unitInCharge": ["cjna2jitPngp6yIV63cdi9"],
    }
