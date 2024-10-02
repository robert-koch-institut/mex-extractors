from mex.common.models import ExtractedAccessPlatform
from mex.common.types import AssetsPath, TechnicalAccessibility, TextLanguage
from mex.extractors.mapping.extract import extract_mapping_data


def test_get_mapping_model() -> None:
    mapping_path = AssetsPath("assets/mappings/__final__/odk/access-platform.yaml")

    mapping_model = extract_mapping_data(mapping_path, ExtractedAccessPlatform)

    expected = {
        "hadPrimarySource": [
            {
                "fieldInPrimarySource": "n/a",
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
                "fieldInPrimarySource": "n/a",
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
                "fieldInPrimarySource": "n/a",
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
                "fieldInPrimarySource": "n/a",
                "locationInPrimarySource": None,
                "examplesInPrimarySource": None,
                "mappingRules": [
                    {
                        "forValues": None,
                        "setValues": [TechnicalAccessibility["INTERNAL"]],
                        "rule": None,
                    }
                ],
                "comment": "internal",
            }
        ],
        "title": [
            {
                "fieldInPrimarySource": "n/a",
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
                "fieldInPrimarySource": "n/a",
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
    assert mapping_model == expected
