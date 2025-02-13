from mex.common.models import AccessPlatformMapping
from mex.common.types import AssetsPath, TextLanguage
from mex.extractors.mapping.extract import extract_mapping_data


def test_get_mapping_model() -> None:
    mapping_path = AssetsPath("assets/mappings/odk/access-platform.yaml")

    raw_mapping_data = extract_mapping_data(mapping_path)
    mapping_model = AccessPlatformMapping.model_validate(raw_mapping_data)

    expected = {
        "hadPrimarySource": [
            {
                "locationInPrimarySource": None,
                "examplesInPrimarySource": None,
                "mappingRules": [
                    {
                        "forValues": None,
                        "setValues": None,
                        "rule": "Assign 'stable target id' of primary source with identifier 'nokeda' in /raw-data/primary-sources/primary-sources.json.",
                    }
                ],
                "comment": None,
            }
        ],
        "identifierInPrimarySource": [
            {
                "locationInPrimarySource": None,
                "examplesInPrimarySource": None,
                "mappingRules": [
                    {
                        "forValues": None,
                        "setValues": ["sumo-db"],
                        "rule": "Use value as it is.",
                    }
                ],
                "comment": None,
            }
        ],
        "alternativeTitle": None,
        "contact": [
            {
                "locationInPrimarySource": None,
                "examplesInPrimarySource": None,
                "mappingRules": [
                    {
                        "forValues": ["Roland Resolved"],
                        "setValues": None,
                        "rule": "Match value using ldap extractor.",
                    }
                ],
                "comment": None,
            }
        ],
        "description": None,
        "endpointDescription": None,
        "endpointType": None,
        "endpointURL": None,
        "landingPage": None,
        "technicalAccessibility": [
            {
                "locationInPrimarySource": None,
                "examplesInPrimarySource": None,
                "mappingRules": [
                    {
                        "forValues": None,
                        "setValues": [
                            "https://mex.rki.de/item/technical-accessibility-1"
                        ],
                        "rule": None,
                    }
                ],
                "comment": "internal",
            }
        ],
        "title": [
            {
                "locationInPrimarySource": None,
                "examplesInPrimarySource": None,
                "mappingRules": [
                    {
                        "forValues": None,
                        "setValues": [
                            {"value": "SUMO Datenbank", "language": TextLanguage.DE}
                        ],
                        "rule": None,
                    }
                ],
                "comment": None,
            }
        ],
        "unitInCharge": [
            {
                "locationInPrimarySource": None,
                "examplesInPrimarySource": None,
                "mappingRules": [
                    {
                        "forValues": ["Abteilung"],
                        "setValues": None,
                        "rule": "Use value to match with identifier in /raw-data/organigram/organizational-units.json.",
                    }
                ],
                "comment": None,
            }
        ],
    }
    assert mapping_model.model_dump() == expected
