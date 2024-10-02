from typing import Any

import pytest

from mex.common.models import (
    ExtractedPrimarySource,
    ExtractedResource,
    ExtractedVariableGroup,
)
from mex.common.primary_source.extract import extract_seed_primary_sources
from mex.common.primary_source.transform import (
    get_primary_sources_by_name,
    transform_seed_primary_sources_to_extracted_primary_sources,
)
from mex.common.types import (
    Link,
    LinkLanguage,
    MergedOrganizationalUnitIdentifier,
    MergedPrimarySourceIdentifier,
    MergedResourceIdentifier,
    Text,
    TextLanguage,
)
from mex.extractors.ifsg.models.meta_catalogue2item import MetaCatalogue2Item
from mex.extractors.ifsg.models.meta_catalogue2item2schema import (
    MetaCatalogue2Item2Schema,
)
from mex.extractors.ifsg.models.meta_disease import MetaDisease
from mex.extractors.ifsg.models.meta_field import MetaField
from mex.extractors.ifsg.models.meta_item import MetaItem
from mex.extractors.ifsg.models.meta_schema2field import MetaSchema2Field
from mex.extractors.ifsg.models.meta_schema2type import MetaSchema2Type
from mex.extractors.ifsg.models.meta_type import MetaType


@pytest.fixture
def unit_stable_target_ids() -> dict[str, MergedOrganizationalUnitIdentifier]:
    """Mock unit stable target ids."""
    return {"FG99": MergedOrganizationalUnitIdentifier.generate(seed=43)}


@pytest.fixture
def meta_catalogue2item() -> list[MetaCatalogue2Item]:
    return [
        MetaCatalogue2Item(IdCatalogue2Item=0, IdCatalogue=0, IdItem=0),
        MetaCatalogue2Item(IdCatalogue2Item=0, IdCatalogue=0, IdItem=1),
        MetaCatalogue2Item(
            IdCatalogue2Item=1,
            IdCatalogue=1001,
            IdItem=1001,
        ),
    ]


@pytest.fixture
def meta_catalogue2item2schema() -> list[MetaCatalogue2Item2Schema]:
    return [
        MetaCatalogue2Item2Schema(
            IdCatalogue2Item=0,
        )
    ]


@pytest.fixture
def meta_schema2type() -> list[MetaSchema2Type]:
    return [
        MetaSchema2Type(id_schema=1, id_type=0),
        MetaSchema2Type(id_schema=42, id_type=11),
    ]


@pytest.fixture
def meta_schema2field() -> list[MetaSchema2Field]:
    return [
        MetaSchema2Field(id_schema=10, id_field=1),
        MetaSchema2Field(id_schema=10, id_field=2),
    ]


@pytest.fixture
def meta_type() -> list[MetaType]:
    return [
        MetaType(code="ABC", id_type=101, sql_table_name="Disease71ABC"),
        MetaType(code="DEF", id_type=102, sql_table_name="Disease73DEF"),
        MetaType(code="GHI", id_type=103, sql_table_name="Disease73GHI"),
        MetaType(code="DEF", id_type=1, sql_table_name="Disease"),
    ]


@pytest.fixture
def meta_item() -> list[MetaItem]:
    return [
        MetaItem(
            IdItem=0,
            ItemName="NullItem",
            ItemNameEN=None,
        ),
        MetaItem(
            IdItem=1,
            ItemName="NullItem2",
            ItemNameEN=None,
        ),
        MetaItem(
            IdItem=1001, ItemName="-nicht erhoben-", ItemNameEN="- not enquired -,"
        ),
    ]


@pytest.fixture
def meta_disease() -> list[MetaDisease]:
    return [
        MetaDisease(
            IdType=101,
            DiseaseName="virus",
            DiseaseNameEN="Epidemic",
            SpecimenName="virus",
            IfSGBundesland=0,
            InBundesland="01,02",
            ReferenceDefA=0,
            ReferenceDefB=1,
            ReferenceDefC=1,
            ReferenceDefD=0,
            ReferenceDefE=0,
            ICD10Code="A1",
        ),
        MetaDisease(
            IdType=102,
            DiseaseName="virus",
            DiseaseNameEN="Epidemic",
            SpecimenName="virus",
            IfSGBundesland=1,
            InBundesland=None,
            ReferenceDefA=1,
            ReferenceDefB=0,
            ReferenceDefC=0,
            ReferenceDefD=1,
            ReferenceDefE=1,
            ICD10Code="A1",
        ),
        MetaDisease(
            IdType=103,
            DiseaseName="virus",
            DiseaseNameEN="Epidemic",
            SpecimenName="virus",
            IfSGBundesland=1,
            InBundesland="07,09",
            ReferenceDefA=1,
            ReferenceDefB=0,
            ReferenceDefC=0,
            ReferenceDefD=1,
            ReferenceDefE=1,
            ICD10Code="A1",
        ),
    ]


@pytest.fixture
def meta_field() -> list[MetaField]:
    return [
        MetaField(
            gui_text="Id der Version",
            gui_tool_tip="lokaler",
            id_catalogue=0,
            id_type=101,
            id_data_type=2,
            id_field=1,
            id_field_type=3,
            to_transport=0,
            sort=1,
            statement_area_group="Epi",
        ),
        MetaField(
            gui_text="Guid des Datensatzes",
            gui_tool_tip="globaler",
            id_catalogue=-12,
            id_type=101,
            id_data_type=1,
            id_field=2,
            id_field_type=2,
            to_transport=0,
            sort=2,
            statement_area_group=None,
        ),
    ]


@pytest.fixture
def ifsg_variable_group() -> dict[str, Any]:
    return {
        "label": [
            {
                "fieldInPrimarySource": 'StatementAreaGroup="Epi|Technical|Clinical|Outbreak|Patient|AdditionalAttributes|NULL|Event|General|Labor|Address"\'',
                "locationInPrimarySource": "Meta.Field",
                "mappingRules": [
                    {
                        "forValues": ["Epi"],
                        "setValues": [
                            {
                                "language": "de",
                                "value": "Epidemiologische Informationen",
                            }
                        ],
                    },
                    {
                        "forValues": ["Technical"],
                        "setValues": [
                            {"language": "de", "value": "Technische Angaben"}
                        ],
                    },
                    {
                        "forValues": ["Clinical"],
                        "setValues": [
                            {"language": "de", "value": "Klinische Informationen"}
                        ],
                    },
                    {
                        "forValues": ["Outbreak"],
                        "setValues": [
                            {"language": "de", "value": "Informationen zum Ausbruch"}
                        ],
                    },
                    {
                        "forValues": ["Patient"],
                        "setValues": [
                            {"language": "de", "value": "Patienteninformationen"}
                        ],
                    },
                    {
                        "forValues": ["Event"],
                        "setValues": [
                            {"language": "de", "value": "Informationen zum Ereignis"}
                        ],
                    },
                    {
                        "forValues": ["General"],
                        "setValues": [
                            {"language": "de", "value": "Administrative Angaben"}
                        ],
                    },
                    {
                        "forValues": ["Labor"],
                        "setValues": [
                            {"language": "de", "value": "Laborinformationen"}
                        ],
                    },
                    {
                        "forValues": ["Address"],
                        "setValues": [
                            {"language": "de", "value": "Adressinformationen"}
                        ],
                    },
                ],
            }
        ]
    }


@pytest.fixture
def resource_parent() -> dict[str, Any]:
    return {
        "accessRestriction": [
            {
                "comment": "restriktiv",
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {"setValues": ["https://mex.rki.de/item/access-restriction-2"]}
                ],
            }
        ],
        "accrualPeriodicity": [
            {
                "comment": "täglich",
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {"setValues": ["https://mex.rki.de/item/frequency-15"]}
                ],
            }
        ],
        "alternativeTitle": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {"setValues": [{"language": "de", "value": "IfSG Meldedaten"}]}
                ],
            }
        ],
        "contact": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {
                        "forValues": ["FG99"],
                        "rule": "Use value to match with identifier in "
                        "/raw-data/organigram/organizational-units.json.",
                    }
                ],
            }
        ],
        "description": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {
                        "setValues": [
                            {"language": "de", "value": "Das Infektionsschutzgesetz"}
                        ]
                    }
                ],
            }
        ],
        "identifierInPrimarySource": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {"rule": "Use value as indicated.", "setValues": ["ifsg-parent"]}
                ],
            }
        ],
        "instrumentToolOrApparatus": [],
        "isPartOf": [],
        "keyword": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {
                        "setValues": [
                            {"language": "de", "value": "Infektionsschutzgesetz"}
                        ]
                    },
                    {"setValues": [{"language": "de", "value": "Infektionsschutz"}]},
                ],
            }
        ],
        "language": [
            {
                "comment": "Deutsch",
                "fieldInPrimarySource": "n/a",
                "mappingRules": [{"setValues": ["https://mex.rki.de/item/language-1"]}],
            }
        ],
        "publication": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {
                        "setValues": [
                            {
                                "language": "de",
                                "title": "Infektionsepidemiologisches Jahrbuch",
                                "url": "https://www.rki.de/DE/Content/Infekt/Jahrbuch/jahrbuch_node.html",
                            }
                        ]
                    },
                    {
                        "setValues": [
                            {
                                "language": "de",
                                "title": "Epidemiologisches Bulletin",
                                "url": "https://www.rki.de/DE/Content/Infekt/EpidBull/epid_bull_node.html",
                            }
                        ]
                    },
                    {
                        "setValues": [
                            {
                                "language": "de",
                                "title": "Falldefinitionen",
                                "url": "https://www.rki.de/DE/Content/Infekt/IfSG/Falldefinition/falldefinition_node.html",
                            }
                        ]
                    },
                ],
            }
        ],
        "resourceTypeGeneral": [
            {
                "comment": "Public Health Fachdaten",
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {"setValues": ["https://mex.rki.de/item/resource-type-general-1"]}
                ],
            }
        ],
        "resourceTypeSpecific": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {"setValues": [{"language": "de", "value": "Meldedaten"}]}
                ],
            }
        ],
        "rights": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {"setValues": [{"language": "de", "value": "Gesundheitsdaten."}]}
                ],
            }
        ],
        "spatial": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {"setValues": [{"language": "de", "value": "Deutschland"}]}
                ],
            }
        ],
        "theme": [
            {
                "comment": "Meldewesen, Infektionskrankheiten",
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {"setValues": ["https://mex.rki.de/item/theme-17"]},
                    {"setValues": ["https://mex.rki.de/item/theme-2"]},
                ],
            }
        ],
        "title": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {
                        "setValues": [
                            {
                                "language": "de",
                                "value": "Meldedaten nach "
                                "Infektionsschutzgesetz "
                                "(IfSG)",
                            }
                        ]
                    }
                ],
            }
        ],
        "unitInCharge": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {
                        "forValues": ["FG99"],
                        "rule": "Use value to match identifer "
                        "using "
                        "/raw-data/organigram/organizational-units.json.",
                    }
                ],
            }
        ],
    }


@pytest.fixture
def resource_states() -> list[dict[str, Any]]:
    return [
        {
            "accessRestriction": [
                {
                    "comment": "restriktiv",
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {"setValues": ["https://mex.rki.de/item/access-restriction-2"]}
                    ],
                }
            ],
            "accrualPeriodicity": [
                {
                    "comment": "unregelmäßig",
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {"setValues": ["https://mex.rki.de/item/frequency-17"]}
                    ],
                }
            ],
            "alternativeTitle": [
                {
                    "fieldInPrimarySource": "InBundesland",
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {
                            "forValues": ["01"],
                            "setValues": [
                                {
                                    "language": "de",
                                    "value": "Meldedaten Schleswig-Holstein",
                                }
                            ],
                        },
                        {
                            "forValues": ["02"],
                            "setValues": [
                                {"language": "de", "value": "Meldedaten Hamburg"}
                            ],
                        },
                    ],
                }
            ],
            "contact": [
                {
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [{"forValues": ["FG99"]}],
                }
            ],
            "description": [],
            "identifierInPrimarySource": [
                {
                    "fieldInPrimarySource": "InBundesland",
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {"forValues": ["01"], "setValues": ["01"]},
                        {"forValues": ["02"], "setValues": ["02"]},
                        {"forValues": ["03"], "setValues": ["03"]},
                        {"forValues": ["04"], "setValues": ["04"]},
                        {"forValues": ["05"], "setValues": ["05"]},
                        {"forValues": ["06"], "setValues": ["06"]},
                        {"forValues": ["07"], "setValues": ["07"]},
                        {"forValues": ["08"], "setValues": ["08"]},
                        {"forValues": ["09"], "setValues": ["09"]},
                        {"forValues": ["10"], "setValues": ["10"]},
                        {"forValues": ["11"], "setValues": ["11"]},
                        {"forValues": ["12"], "setValues": ["12"]},
                        {"forValues": ["13"], "setValues": ["13"]},
                        {"forValues": ["14"], "setValues": ["14"]},
                        {"forValues": ["15"], "setValues": ["15"]},
                        {"forValues": ["16"], "setValues": ["16"]},
                    ],
                }
            ],
            "instrumentToolOrApparatus": [],
            "isPartOf": [
                {
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {
                            "rule": "Use the 'stable target id' of item "
                            "described by "
                            "mapping/../ifsg/resource_parent"
                        }
                    ],
                }
            ],
            "keyword": [
                {
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {
                            "setValues": [
                                {"language": "de", "value": "Infektionsschutzgesetz"}
                            ]
                        },
                        {
                            "setValues": [
                                {"language": "de", "value": "Infektionsschutz"}
                            ]
                        },
                    ],
                }
            ],
            "language": [
                {
                    "comment": "Deutsch",
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {"setValues": ["https://mex.rki.de/item/language-1"]}
                    ],
                }
            ],
            "publication": [
                {
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {
                            "setValues": [
                                {
                                    "language": "de",
                                    "title": "Infektionsepidemiologisches Jahrbuch",
                                    "url": "https://www.rki.de/DE/Content/Infekt/Jahrbuch/jahrbuch_node.html",
                                },
                                {
                                    "language": "de",
                                    "title": "Epidemiologisches Bulletin",
                                    "url": "https://www.rki.de/DE/Content/Infekt/EpidBull/epid_bull_node.html",
                                },
                                {
                                    "language": "de",
                                    "title": "Falldefinitionen",
                                    "url": "https://www.rki.de/DE/Content/Infekt/IfSG/Falldefinition/falldefinition_node.html",
                                },
                            ]
                        }
                    ],
                },
                {
                    "fieldInPrimarySource": "InBundesland",
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {
                            "forValues": ["01"],
                            "setValues": [
                                {
                                    "language": "de",
                                    "title": "Rheinland-Pfalz",
                                    "url": "http://landesrecht.rlp.de/jportal/portal/page/bsrlpprod.psml?doc.id=jlr-IfSGMeldpflVRPpP1%3Ajuris-lr00&showdoccase=1&doc.hl=1&documentnumber=1",
                                }
                            ],
                        },
                        {
                            "forValues": ["09"],
                            "setValues": [
                                {
                                    "language": "de",
                                    "title": "Bayern",
                                    "url": "https://www.gesetze-bayern.de/Content/Document/BayMeldePflV/true",
                                }
                            ],
                        },
                        {
                            "forValues": ["10"],
                            "setValues": [
                                {
                                    "language": "de",
                                    "title": "Saarland",
                                    "url": "https://www.saarland.de/msgff/DE/portale/gesundheitundpraevention/leistungenabisz/gesundheitsschutz/infektionsschutzgesetz/infektionsschutzgesetz.html",
                                }
                            ],
                        },
                        {
                            "forValues": ["11"],
                            "setValues": [
                                {
                                    "language": "de",
                                    "title": "Berlin",
                                    "url": "https://gesetze.berlin.de/bsbe/document/jlr-IfSGMeldpflVBEpP1",
                                }
                            ],
                        },
                        {
                            "forValues": ["12"],
                            "setValues": [
                                {
                                    "language": "de",
                                    "title": "Brandenburg",
                                    "url": "https://bravors.brandenburg.de/verordnungen/infkrankmv_2016",
                                }
                            ],
                        },
                        {
                            "forValues": ["13"],
                            "setValues": [
                                {
                                    "language": "de",
                                    "title": "Mecklenburg-Vorpommern",
                                    "url": "https://www.landesrecht-mv.de/bsmv/document/jlr-InfSchAGMVrahmen",
                                }
                            ],
                        },
                        {
                            "forValues": ["14"],
                            "setValues": [
                                {
                                    "language": "de",
                                    "title": "Sachsen",
                                    "url": "https://www.revosax.sachsen.de/vorschrift/1307-IfSGMeldeVO",
                                }
                            ],
                        },
                        {
                            "forValues": ["15"],
                            "setValues": [
                                {
                                    "language": "de",
                                    "title": "Sachsen-Anhalt",
                                    "url": "https://www.landesrecht.sachsen-anhalt.de/bsst/document/jlr-IfSGMeldpflVST2005rahmen",
                                }
                            ],
                        },
                        {
                            "forValues": ["16"],
                            "setValues": [
                                {
                                    "language": "de",
                                    "title": "Thüringen",
                                    "url": "https://landesrecht.thueringen.de/bsth/document/jlr-IfKrMeldAnpVTHrahmen",
                                }
                            ],
                        },
                    ],
                },
            ],
            "resourceTypeGeneral": [
                {
                    "comment": "Public Health Fachdaten",
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {
                            "setValues": [
                                "https://mex.rki.de/item/resource-type-general-1"
                            ]
                        }
                    ],
                }
            ],
            "resourceTypeSpecific": [
                {
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {"setValues": [{"language": "de", "value": "Meldedaten"}]}
                    ],
                }
            ],
            "rights": [
                {
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {
                            "setValues": [
                                {"language": "de", "value": "Gesundheitsdaten."}
                            ]
                        }
                    ],
                }
            ],
            "spatial": [
                {
                    "fieldInPrimarySource": "InBundesland",
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {
                            "forValues": ["01"],
                            "setValues": [
                                {"language": "de", "value": "Schleswig-Holstein"}
                            ],
                        },
                        {
                            "forValues": ["02"],
                            "setValues": [{"language": "de", "value": "Hamburg"}],
                        },
                        {
                            "forValues": ["03"],
                            "setValues": [{"language": "de", "value": "Niedersachsen"}],
                        },
                        {
                            "forValues": ["04"],
                            "setValues": [{"language": "de", "value": "Bremen"}],
                        },
                        {
                            "forValues": ["05"],
                            "setValues": [
                                {"language": "de", "value": "Nordrhein-Westfalen"}
                            ],
                        },
                        {
                            "forValues": ["06"],
                            "setValues": [{"language": "de", "value": "Hessen"}],
                        },
                        {
                            "forValues": ["07"],
                            "setValues": [
                                {"language": "de", "value": "Rheinland-Pfalz"}
                            ],
                        },
                        {
                            "forValues": ["08"],
                            "setValues": [
                                {"language": "de", "value": "Baden-Württemberg"}
                            ],
                        },
                        {
                            "forValues": ["09"],
                            "setValues": [{"language": "de", "value": "Bayern"}],
                        },
                        {
                            "forValues": ["10"],
                            "setValues": [{"language": "de", "value": "Saarland"}],
                        },
                        {
                            "forValues": ["11"],
                            "setValues": [{"language": "de", "value": "Berlin"}],
                        },
                        {
                            "forValues": ["12"],
                            "setValues": [{"language": "de", "value": "Brandenburg"}],
                        },
                        {
                            "forValues": ["13"],
                            "setValues": [
                                {"language": "de", "value": "Mecklenburg-Vorpommern"}
                            ],
                        },
                        {
                            "forValues": ["14"],
                            "setValues": [{"language": "de", "value": "Sachsen"}],
                        },
                        {
                            "forValues": ["15"],
                            "setValues": [
                                {"language": "de", "value": "Sachsen-Anhalt"}
                            ],
                        },
                        {
                            "forValues": ["16"],
                            "setValues": [{"language": "de", "value": "Thüringen"}],
                        },
                    ],
                }
            ],
            "theme": [
                {
                    "comment": "Meldewesen, Infektionskrankheiten",
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {"setValues": ["https://mex.rki.de/item/theme-17"]},
                        {"setValues": ["https://mex.rki.de/item/theme-2"]},
                    ],
                }
            ],
            "title": [
                {
                    "fieldInPrimarySource": "InBundesland",
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {
                            "forValues": ["01"],
                            "setValues": [
                                {"language": "de", "value": "Schleswig-Holstein"}
                            ],
                        },
                        {
                            "forValues": ["02"],
                            "setValues": [{"language": "de", "value": "Hamburg"}],
                        },
                        {
                            "forValues": ["03"],
                            "setValues": [{"language": "de", "value": "Niedersachsen"}],
                        },
                        {
                            "forValues": ["04"],
                            "setValues": [{"language": "de", "value": "Bremen"}],
                        },
                        {
                            "forValues": ["05"],
                            "setValues": [
                                {"language": "de", "value": "Nordrhein-Westfalen"}
                            ],
                        },
                        {
                            "forValues": ["06"],
                            "setValues": [{"language": "de", "value": "Hessen"}],
                        },
                        {
                            "forValues": ["07"],
                            "setValues": [
                                {"language": "de", "value": "Rheinland-Pfalz"}
                            ],
                        },
                        {
                            "forValues": ["08"],
                            "setValues": [
                                {"language": "de", "value": "Baden-Württemberg"}
                            ],
                        },
                        {
                            "forValues": ["09"],
                            "setValues": [{"language": "de", "value": "Bayern"}],
                        },
                        {
                            "forValues": ["10"],
                            "setValues": [{"language": "de", "value": "Saarland"}],
                        },
                        {
                            "forValues": ["11"],
                            "setValues": [{"language": "de", "value": "Berlin."}],
                        },
                        {
                            "forValues": ["12"],
                            "setValues": [{"language": "de", "value": "Brandenburg."}],
                        },
                        {
                            "forValues": ["13"],
                            "setValues": [
                                {"language": "de", "value": "Mecklenburg-Vorpommern."}
                            ],
                        },
                        {
                            "forValues": ["14"],
                            "setValues": [{"language": "de", "value": "Sachsen."}],
                        },
                        {
                            "forValues": ["15"],
                            "setValues": [
                                {"language": "de", "value": "Sachsen-Anhalt."}
                            ],
                        },
                        {
                            "forValues": ["16"],
                            "setValues": [{"language": "de", "value": "Thüringen."}],
                        },
                    ],
                }
            ],
            "unitInCharge": [
                {
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {
                            "forValues": ["FG99"],
                            "rule": "Use value to match with "
                            "identifer in "
                            "/raw-data/organigram/organizational-units.json.",
                        }
                    ],
                }
            ],
        },
        {
            "accessRestriction": [
                {
                    "comment": "restriktiv",
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {"setValues": ["https://mex.rki.de/item/access-restriction-2"]}
                    ],
                }
            ],
            "accrualPeriodicity": [
                {
                    "comment": "unregelmäßig",
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {"setValues": ["https://mex.rki.de/item/frequency-17"]}
                    ],
                }
            ],
            "alternativeTitle": [
                {
                    "fieldInPrimarySource": "InBundesland",
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {
                            "forValues": ["01"],
                            "setValues": [
                                {
                                    "language": "de",
                                    "value": "Meldedaten Schleswig-Holstein",
                                }
                            ],
                        },
                        {
                            "forValues": ["02"],
                            "setValues": [
                                {"language": "de", "value": "Meldedaten Hamburg"}
                            ],
                        },
                    ],
                }
            ],
            "contact": [
                {
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [{"forValues": ["FG99"]}],
                }
            ],
            "description": [],
            "identifierInPrimarySource": [
                {
                    "fieldInPrimarySource": "InBundesland",
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {"forValues": ["01"], "setValues": ["01"]},
                        {"forValues": ["02"], "setValues": ["02"]},
                        {"forValues": ["03"], "setValues": ["03"]},
                        {"forValues": ["04"], "setValues": ["04"]},
                        {"forValues": ["05"], "setValues": ["05"]},
                        {"forValues": ["06"], "setValues": ["06"]},
                        {"forValues": ["07"], "setValues": ["07"]},
                        {"forValues": ["08"], "setValues": ["08"]},
                        {"forValues": ["09"], "setValues": ["09"]},
                        {"forValues": ["10"], "setValues": ["10"]},
                        {"forValues": ["11"], "setValues": ["11"]},
                        {"forValues": ["12"], "setValues": ["12"]},
                        {"forValues": ["13"], "setValues": ["13"]},
                        {"forValues": ["14"], "setValues": ["14"]},
                        {"forValues": ["15"], "setValues": ["15"]},
                        {"forValues": ["16"], "setValues": ["16"]},
                    ],
                }
            ],
            "instrumentToolOrApparatus": [],
            "isPartOf": [
                {
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {
                            "rule": "Use the 'stable target id' of item "
                            "described by "
                            "mapping/../ifsg/resource_parent"
                        }
                    ],
                }
            ],
            "keyword": [
                {
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {
                            "setValues": [
                                {"language": "de", "value": "Infektionsschutzgesetz"}
                            ]
                        },
                        {
                            "setValues": [
                                {"language": "de", "value": "Infektionsschutz"}
                            ]
                        },
                    ],
                }
            ],
            "language": [
                {
                    "comment": "Deutsch",
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {"setValues": ["https://mex.rki.de/item/language-1"]}
                    ],
                }
            ],
            "publication": [
                {
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {
                            "setValues": [
                                {
                                    "language": "de",
                                    "title": "Infektionsepidemiologisches Jahrbuch",
                                    "url": "https://www.rki.de/DE/Content/Infekt/Jahrbuch/jahrbuch_node.html",
                                },
                                {
                                    "language": "de",
                                    "title": "Epidemiologisches Bulletin",
                                    "url": "https://www.rki.de/DE/Content/Infekt/EpidBull/epid_bull_node.html",
                                },
                                {
                                    "language": "de",
                                    "title": "Falldefinitionen",
                                    "url": "https://www.rki.de/DE/Content/Infekt/IfSG/Falldefinition/falldefinition_node.html",
                                },
                            ]
                        }
                    ],
                },
                {
                    "fieldInPrimarySource": "InBundesland",
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {
                            "forValues": ["07"],
                            "setValues": [
                                {
                                    "language": "de",
                                    "title": "Rheinland-Pfalz",
                                    "url": "http://landesrecht.rlp.de/jportal/portal/page/bsrlpprod.psml?doc.id=jlr-IfSGMeldpflVRPpP1%3Ajuris-lr00&showdoccase=1&doc.hl=1&documentnumber=1",
                                }
                            ],
                        },
                        {
                            "forValues": ["09"],
                            "setValues": [
                                {
                                    "language": "de",
                                    "title": "Bayern",
                                    "url": "https://www.gesetze-bayern.de/Content/Document/BayMeldePflV/true",
                                }
                            ],
                        },
                        {
                            "forValues": ["10"],
                            "setValues": [
                                {
                                    "language": "de",
                                    "title": "Saarland",
                                    "url": "https://www.saarland.de/msgff/DE/portale/gesundheitundpraevention/leistungenabisz/gesundheitsschutz/infektionsschutzgesetz/infektionsschutzgesetz.html",
                                }
                            ],
                        },
                        {
                            "forValues": ["11"],
                            "setValues": [
                                {
                                    "language": "de",
                                    "title": "Berlin",
                                    "url": "https://gesetze.berlin.de/bsbe/document/jlr-IfSGMeldpflVBEpP1",
                                }
                            ],
                        },
                        {
                            "forValues": ["12"],
                            "setValues": [
                                {
                                    "language": "de",
                                    "title": "Brandenburg",
                                    "url": "https://bravors.brandenburg.de/verordnungen/infkrankmv_2016",
                                }
                            ],
                        },
                        {
                            "forValues": ["13"],
                            "setValues": [
                                {
                                    "language": "de",
                                    "title": "Mecklenburg-Vorpommern",
                                    "url": "https://www.landesrecht-mv.de/bsmv/document/jlr-InfSchAGMVrahmen",
                                }
                            ],
                        },
                        {
                            "forValues": ["14"],
                            "setValues": [
                                {
                                    "language": "de",
                                    "title": "Sachsen",
                                    "url": "https://www.revosax.sachsen.de/vorschrift/1307-IfSGMeldeVO",
                                }
                            ],
                        },
                        {
                            "forValues": ["15"],
                            "setValues": [
                                {
                                    "language": "de",
                                    "title": "Sachsen-Anhalt",
                                    "url": "https://www.landesrecht.sachsen-anhalt.de/bsst/document/jlr-IfSGMeldpflVST2005rahmen",
                                }
                            ],
                        },
                        {
                            "forValues": ["16"],
                            "setValues": [
                                {
                                    "language": "de",
                                    "title": "Thüringen",
                                    "url": "https://landesrecht.thueringen.de/bsth/document/jlr-IfKrMeldAnpVTHrahmen",
                                }
                            ],
                        },
                    ],
                },
            ],
            "resourceTypeGeneral": [
                {
                    "comment": "Public Health Fachdaten",
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {
                            "setValues": [
                                "https://mex.rki.de/item/resource-type-general-1"
                            ]
                        }
                    ],
                }
            ],
            "resourceTypeSpecific": [
                {
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {"setValues": [{"language": "de", "value": "Meldedaten"}]}
                    ],
                }
            ],
            "rights": [
                {
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {
                            "setValues": [
                                {"language": "de", "value": "Gesundheitsdaten."}
                            ]
                        }
                    ],
                }
            ],
            "spatial": [
                {
                    "fieldInPrimarySource": "InBundesland",
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {
                            "forValues": ["01"],
                            "setValues": [
                                {"language": "de", "value": "Schleswig-Holstein"}
                            ],
                        },
                        {
                            "forValues": ["02"],
                            "setValues": [{"language": "de", "value": "Hamburg"}],
                        },
                        {
                            "forValues": ["03"],
                            "setValues": [{"language": "de", "value": "Niedersachsen"}],
                        },
                        {
                            "forValues": ["04"],
                            "setValues": [{"language": "de", "value": "Bremen"}],
                        },
                        {
                            "forValues": ["05"],
                            "setValues": [
                                {"language": "de", "value": "Nordrhein-Westfalen"}
                            ],
                        },
                        {
                            "forValues": ["06"],
                            "setValues": [{"language": "de", "value": "Hessen"}],
                        },
                        {
                            "forValues": ["07"],
                            "setValues": [
                                {"language": "de", "value": "Rheinland-Pfalz"}
                            ],
                        },
                        {
                            "forValues": ["08"],
                            "setValues": [
                                {"language": "de", "value": "Baden-Württemberg"}
                            ],
                        },
                        {
                            "forValues": ["09"],
                            "setValues": [{"language": "de", "value": "Bayern"}],
                        },
                        {
                            "forValues": ["10"],
                            "setValues": [{"language": "de", "value": "Saarland"}],
                        },
                        {
                            "forValues": ["11"],
                            "setValues": [{"language": "de", "value": "Berlin"}],
                        },
                        {
                            "forValues": ["12"],
                            "setValues": [{"language": "de", "value": "Brandenburg"}],
                        },
                        {
                            "forValues": ["13"],
                            "setValues": [
                                {"language": "de", "value": "Mecklenburg-Vorpommern"}
                            ],
                        },
                        {
                            "forValues": ["14"],
                            "setValues": [{"language": "de", "value": "Sachsen"}],
                        },
                        {
                            "forValues": ["15"],
                            "setValues": [
                                {"language": "de", "value": "Sachsen-Anhalt"}
                            ],
                        },
                        {
                            "forValues": ["16"],
                            "setValues": [{"language": "de", "value": "Thüringen"}],
                        },
                    ],
                }
            ],
            "theme": [
                {
                    "comment": "Meldewesen, Infektionskrankheiten",
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {"setValues": ["https://mex.rki.de/item/theme-17"]},
                        {"setValues": ["https://mex.rki.de/item/theme-2"]},
                    ],
                }
            ],
            "title": [
                {
                    "fieldInPrimarySource": "InBundesland",
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {
                            "forValues": ["01"],
                            "setValues": [
                                {"language": "de", "value": "Schleswig-Holstein"}
                            ],
                        },
                        {
                            "forValues": ["02"],
                            "setValues": [{"language": "de", "value": "Hamburg"}],
                        },
                        {
                            "forValues": ["03"],
                            "setValues": [{"language": "de", "value": "Niedersachsen"}],
                        },
                        {
                            "forValues": ["04"],
                            "setValues": [{"language": "de", "value": "Bremen"}],
                        },
                        {
                            "forValues": ["05"],
                            "setValues": [
                                {"language": "de", "value": "Nordrhein-Westfalen"}
                            ],
                        },
                        {
                            "forValues": ["06"],
                            "setValues": [{"language": "de", "value": "Hessen"}],
                        },
                        {
                            "forValues": ["07"],
                            "setValues": [
                                {"language": "de", "value": "Rheinland-Pfalz"}
                            ],
                        },
                        {
                            "forValues": ["08"],
                            "setValues": [
                                {"language": "de", "value": "Baden-Württemberg"}
                            ],
                        },
                        {
                            "forValues": ["09"],
                            "setValues": [{"language": "de", "value": "Bayern"}],
                        },
                        {
                            "forValues": ["10"],
                            "setValues": [{"language": "de", "value": "Saarland"}],
                        },
                        {
                            "forValues": ["11"],
                            "setValues": [{"language": "de", "value": "Berlin."}],
                        },
                        {
                            "forValues": ["12"],
                            "setValues": [{"language": "de", "value": "Brandenburg."}],
                        },
                        {
                            "forValues": ["13"],
                            "setValues": [
                                {"language": "de", "value": "Mecklenburg-Vorpommern."}
                            ],
                        },
                        {
                            "forValues": ["14"],
                            "setValues": [{"language": "de", "value": "Sachsen."}],
                        },
                        {
                            "forValues": ["15"],
                            "setValues": [
                                {"language": "de", "value": "Sachsen-Anhalt."}
                            ],
                        },
                        {
                            "forValues": ["16"],
                            "setValues": [{"language": "de", "value": "Thüringen."}],
                        },
                    ],
                }
            ],
            "unitInCharge": [
                {
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {
                            "forValues": ["FG99"],
                            "rule": "Use value to match with "
                            "identifer in "
                            "/raw-data/organigram/organizational-units.json.",
                        }
                    ],
                }
            ],
        },
    ]


@pytest.fixture
def resource_diseases() -> list[dict[str, Any]]:
    return [
        {
            "accessRestriction": [
                {
                    "comment": "restriktiv",
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {"setValues": ["https://mex.rki.de/item/access-restriction-2"]}
                    ],
                }
            ],
            "accrualPeriodicity": [
                {
                    "comment": "unregelmäßig",
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {"setValues": ["https://mex.rki.de/item/frequency-17"]}
                    ],
                }
            ],
            "alternativeTitle": [
                {
                    "examplesInPrimarySource": ["BAN", "BOB"],
                    "fieldInPrimarySource": "Code",
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {"rule": "Use value as it is. Do not assign a language."}
                    ],
                }
            ],
            "contact": [
                {
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {
                            "forValues": ["FG99"],
                            "rule": "Use value to match with identifier in "
                            "/raw-data/organigram/organizational-units.json.",
                        }
                    ],
                }
            ],
            "description": [],
            "identifierInPrimarySource": [
                {
                    "examplesInPrimarySource": ["102"],
                    "fieldInPrimarySource": "IdType",
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [{"rule": "Use value as it is."}],
                }
            ],
            "instrumentToolOrApparatus": [
                {
                    "fieldInPrimarySource": 'ReferenceDef[A|B|C|D|E]="1"',
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {
                            "forValues": ["A=1"],
                            "setValues": [
                                {"language": "de", "value": "Falldefinition A"}
                            ],
                        },
                        {
                            "forValues": ["B=1"],
                            "setValues": [
                                {"language": "de", "value": "Falldefinition B"}
                            ],
                        },
                        {
                            "forValues": ["C=1"],
                            "setValues": [
                                {"language": "de", "value": "Falldefinition C"}
                            ],
                        },
                        {
                            "forValues": ["D=1"],
                            "setValues": [
                                {"language": "de", "value": "Falldefinition D"}
                            ],
                        },
                        {
                            "forValues": ["E=1"],
                            "setValues": [
                                {"language": "de", "value": "Falldefinition E"}
                            ],
                        },
                    ],
                }
            ],
            "isPartOf": [
                {
                    "fieldInPrimarySource": "IFSGBundesland",
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {"forValues": ["0"]},
                        {
                            "rule": "Assign 'stable target id' of the "
                            "item described by "
                            "/mappings/../ifsg/resource_parent"
                        },
                    ],
                },
                {
                    "fieldInPrimarySource": 'IFSGBundesland="1" and InBundesland',
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {
                            "forValues": ["07"],
                            "rule": "Assign 'stable target id' of the "
                            "item with "
                            "identiferInPrimarySource='07' as "
                            "described by "
                            "mapping/../ifsg/resource_state",
                        },
                        {
                            "forValues": ["09"],
                            "rule": "Assign 'stable target id' of the "
                            "item with "
                            "identiferInPrimarySource='09' as "
                            "described by "
                            "mapping/../ifsg/resource_state",
                        },
                        {
                            "forValues": ["10"],
                            "rule": "Assign 'stable target id' of the "
                            "item with "
                            "identiferInPrimarySource='10' as "
                            "described by "
                            "mapping/../ifsg/resource_state",
                        },
                        {
                            "forValues": ["11"],
                            "rule": "Assign 'stable target id' of the "
                            "item with "
                            "identiferInPrimarySource='11' as "
                            "described by "
                            "mapping/../ifsg/resource_state",
                        },
                        {
                            "forValues": ["12"],
                            "rule": "Assign 'stable target id' of the "
                            "item with "
                            "identiferInPrimarySource='12' as "
                            "described by "
                            "mapping/../ifsg/resource_state",
                        },
                        {
                            "forValues": ["13"],
                            "rule": "Assign 'stable target id' of the "
                            "item with "
                            "identiferInPrimarySource='13' as  "
                            "described by "
                            "mapping/../ifsg/resource_state",
                        },
                        {
                            "forValues": ["14"],
                            "rule": "Assign 'stable target id' of the "
                            "item with "
                            "identiferInPrimarySource='14' as "
                            "described by "
                            "mapping/../ifsg/resource_state",
                        },
                        {
                            "forValues": ["15"],
                            "rule": "Assign 'stable target id' of the "
                            "item with "
                            "identiferInPrimarySource='15' as "
                            "described by "
                            "mapping/../ifsg/resource_state",
                        },
                        {
                            "forValues": ["16"],
                            "rule": "Assign 'stable target id' of the "
                            "item with "
                            "identiferInPrimarySource='16' as "
                            "described by "
                            "mapping/../ifsg/resource_state",
                        },
                    ],
                },
            ],
            "keyword": [
                {
                    "examplesInPrimarySource": ["Milzbrand", "Borreliose"],
                    "fieldInPrimarySource": "DiseaseName",
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {
                            "rule": "Assign 'de' as default for the "
                            "language property of the text "
                            "object."
                        }
                    ],
                },
                {
                    "examplesInPrimarySource": ["Anthrax", "Lyme disease"],
                    "fieldInPrimarySource": "DiseaseNameEN",
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {
                            "rule": "Assign 'en' as default for the "
                            "language property of the text "
                            "object."
                        }
                    ],
                },
                {
                    "examplesInPrimarySource": [
                        "Bacillus anthracis",
                        "Borelia burgdorferi",
                    ],
                    "fieldInPrimarySource": "SpecimenName",
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {"rule": "Use value as it is. Use language detection."}
                    ],
                },
            ],
            "language": [
                {
                    "comment": "Deutsch",
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {"setValues": ["https://mex.rki.de/item/language-1"]}
                    ],
                }
            ],
            "publication": [
                {
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {
                            "setValues": [
                                {
                                    "language": "de",
                                    "title": "Falldefinitionen",
                                    "url": "https://www.rki.de/DE/Content/Infekt/IfSG/Falldefinition/falldefinition_node.html",
                                }
                            ]
                        }
                    ],
                }
            ],
            "resourceTypeGeneral": [
                {
                    "comment": "Public Health Fachdaten",
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {
                            "setValues": [
                                "https://mex.rki.de/item/resource-type-general-1"
                            ]
                        }
                    ],
                }
            ],
            "resourceTypeSpecific": [
                {
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {"setValues": [{"language": "de", "value": "Meldedaten"}]}
                    ],
                }
            ],
            "rights": [
                {
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {
                            "setValues": [
                                {"language": "de", "value": "Gesundheitsdaten."}
                            ]
                        }
                    ],
                }
            ],
            "spatial": [
                {
                    "fieldInPrimarySource": 'IFSGBundesland="0"',
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {"setValues": [{"language": "de", "value": "Deutschland"}]}
                    ],
                },
                {
                    "fieldInPrimarySource": 'IFSGBundesland="1" and InBundesland',
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {
                            "forValues": ["07"],
                            "setValues": [
                                {"language": "de", "value": "Rheinland-Pfalz"}
                            ],
                        },
                        {
                            "forValues": ["09"],
                            "setValues": [{"language": "de", "value": "Bayern"}],
                        },
                        {
                            "forValues": ["10"],
                            "setValues": [{"language": "de", "value": "Saarland"}],
                        },
                        {
                            "forValues": ["11"],
                            "setValues": [{"language": "de", "value": "Berlin"}],
                        },
                        {
                            "forValues": ["12"],
                            "setValues": [{"language": "de", "value": "Brandenburg"}],
                        },
                        {
                            "forValues": ["13"],
                            "setValues": [
                                {"language": "de", "value": "Mecklenburg-Vorpommern"}
                            ],
                        },
                        {
                            "forValues": ["14"],
                            "setValues": [{"language": "de", "value": "Sachsen"}],
                        },
                        {
                            "forValues": ["15"],
                            "setValues": [
                                {"language": "de", "value": "Sachsen-Anhalt"}
                            ],
                        },
                        {
                            "forValues": ["16"],
                            "setValues": [{"language": "de", "value": "Thüringen"}],
                        },
                    ],
                },
            ],
            "theme": [
                {
                    "comment": "Meldewesen, Infektionskrankheiten",
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {
                            "setValues": [
                                "https://mex.rki.de/item/theme-17",
                                "https://mex.rki.de/item/theme-2",
                            ]
                        }
                    ],
                }
            ],
            "title": [
                {
                    "fieldInPrimarySource": "DiseaseName",
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {
                            "rule": "Construct the title after the following "
                            "schema: Meldedaten nach "
                            "Infektionsschutzgesetz (IfSG) zu "
                            "[DiseaseName]. Assign 'de' as default "
                            "for the language property of the text "
                            "object."
                        }
                    ],
                }
            ],
            "unitInCharge": [
                {
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {
                            "forValues": ["FG99"],
                            "rule": "Use value to match with "
                            "identifier from "
                            "/raw-data/organigram/organizational-units.json.",
                        }
                    ],
                }
            ],
        },
        {
            "accessRestriction": [
                {
                    "comment": "restriktiv",
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {"setValues": ["https://mex.rki.de/item/access-restriction-2"]}
                    ],
                }
            ],
            "accrualPeriodicity": [
                {
                    "comment": "unregelmäßig",
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {"setValues": ["https://mex.rki.de/item/frequency-17"]}
                    ],
                }
            ],
            "alternativeTitle": [
                {
                    "examplesInPrimarySource": ["BAN", "BOB"],
                    "fieldInPrimarySource": "Code",
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {"rule": "Use value as it is. Do not assign a language."}
                    ],
                }
            ],
            "contact": [
                {
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {
                            "forValues": ["FG99"],
                            "rule": "Use value to match with identifier in "
                            "/raw-data/organigram/organizational-units.json.",
                        }
                    ],
                }
            ],
            "description": [],
            "identifierInPrimarySource": [
                {
                    "examplesInPrimarySource": ["102"],
                    "fieldInPrimarySource": "IdType",
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [{"rule": "Use value as it is."}],
                }
            ],
            "instrumentToolOrApparatus": [
                {
                    "fieldInPrimarySource": 'ReferenceDef[A|B|C|D|E]="1"',
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {
                            "forValues": ["A=1"],
                            "setValues": [
                                {"language": "de", "value": "Falldefinition A"}
                            ],
                        },
                        {
                            "forValues": ["B=1"],
                            "setValues": [
                                {"language": "de", "value": "Falldefinition B"}
                            ],
                        },
                        {
                            "forValues": ["C=1"],
                            "setValues": [
                                {"language": "de", "value": "Falldefinition C"}
                            ],
                        },
                        {
                            "forValues": ["D=1"],
                            "setValues": [
                                {"language": "de", "value": "Falldefinition D"}
                            ],
                        },
                        {
                            "forValues": ["E=1"],
                            "setValues": [
                                {"language": "de", "value": "Falldefinition E"}
                            ],
                        },
                    ],
                }
            ],
            "isPartOf": [
                {
                    "fieldInPrimarySource": "IFSGBundesland",
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {"forValues": ["0"]},
                        {
                            "rule": "Assign 'stable target id' of the "
                            "item described by "
                            "/mappings/../ifsg/resource_parent"
                        },
                    ],
                },
                {
                    "fieldInPrimarySource": 'IFSGBundesland="1" and InBundesland',
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {
                            "forValues": ["07"],
                            "rule": "Assign 'stable target id' of the "
                            "item with "
                            "identiferInPrimarySource='07' as "
                            "described by "
                            "mapping/../ifsg/resource_state",
                        },
                        {
                            "forValues": ["09"],
                            "rule": "Assign 'stable target id' of the "
                            "item with "
                            "identiferInPrimarySource='09' as "
                            "described by "
                            "mapping/../ifsg/resource_state",
                        },
                        {
                            "forValues": ["10"],
                            "rule": "Assign 'stable target id' of the "
                            "item with "
                            "identiferInPrimarySource='10' as "
                            "described by "
                            "mapping/../ifsg/resource_state",
                        },
                        {
                            "forValues": ["11"],
                            "rule": "Assign 'stable target id' of the "
                            "item with "
                            "identiferInPrimarySource='11' as "
                            "described by "
                            "mapping/../ifsg/resource_state",
                        },
                        {
                            "forValues": ["12"],
                            "rule": "Assign 'stable target id' of the "
                            "item with "
                            "identiferInPrimarySource='12' as "
                            "described by "
                            "mapping/../ifsg/resource_state",
                        },
                        {
                            "forValues": ["13"],
                            "rule": "Assign 'stable target id' of the "
                            "item with "
                            "identiferInPrimarySource='13' as  "
                            "described by "
                            "mapping/../ifsg/resource_state",
                        },
                        {
                            "forValues": ["14"],
                            "rule": "Assign 'stable target id' of the "
                            "item with "
                            "identiferInPrimarySource='14' as "
                            "described by "
                            "mapping/../ifsg/resource_state",
                        },
                        {
                            "forValues": ["15"],
                            "rule": "Assign 'stable target id' of the "
                            "item with "
                            "identiferInPrimarySource='15' as "
                            "described by "
                            "mapping/../ifsg/resource_state",
                        },
                        {
                            "forValues": ["16"],
                            "rule": "Assign 'stable target id' of the "
                            "item with "
                            "identiferInPrimarySource='16' as "
                            "described by "
                            "mapping/../ifsg/resource_state",
                        },
                    ],
                },
            ],
            "keyword": [
                {
                    "examplesInPrimarySource": ["Milzbrand", "Borreliose"],
                    "fieldInPrimarySource": "DiseaseName",
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {
                            "rule": "Assign 'de' as default for the "
                            "language property of the text "
                            "object."
                        }
                    ],
                },
                {
                    "examplesInPrimarySource": ["Anthrax", "Lyme disease"],
                    "fieldInPrimarySource": "DiseaseNameEN",
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {
                            "rule": "Assign 'en' as default for the "
                            "language property of the text "
                            "object."
                        }
                    ],
                },
                {
                    "examplesInPrimarySource": [
                        "Bacillus anthracis",
                        "Borelia burgdorferi",
                    ],
                    "fieldInPrimarySource": "SpecimenName",
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {"rule": "Use value as it is. Use language detection."}
                    ],
                },
            ],
            "language": [
                {
                    "comment": "Deutsch",
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {"setValues": ["https://mex.rki.de/item/language-1"]}
                    ],
                }
            ],
            "publication": [
                {
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {
                            "setValues": [
                                {
                                    "language": "de",
                                    "title": "Falldefinitionen",
                                    "url": "https://www.rki.de/DE/Content/Infekt/IfSG/Falldefinition/falldefinition_node.html",
                                }
                            ]
                        }
                    ],
                }
            ],
            "resourceTypeGeneral": [
                {
                    "comment": "Public Health Fachdaten",
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {
                            "setValues": [
                                "https://mex.rki.de/item/resource-type-general-1"
                            ]
                        }
                    ],
                }
            ],
            "resourceTypeSpecific": [
                {
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {"setValues": [{"language": "de", "value": "Meldedaten"}]}
                    ],
                }
            ],
            "rights": [
                {
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {
                            "setValues": [
                                {"language": "de", "value": "Gesundheitsdaten."}
                            ]
                        }
                    ],
                }
            ],
            "spatial": [
                {
                    "fieldInPrimarySource": 'IFSGBundesland="0"',
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {"setValues": [{"language": "de", "value": "Deutschland"}]}
                    ],
                },
                {
                    "fieldInPrimarySource": 'IFSGBundesland="1" and InBundesland',
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {
                            "forValues": ["07"],
                            "setValues": [
                                {"language": "de", "value": "Rheinland-Pfalz"}
                            ],
                        },
                        {
                            "forValues": ["09"],
                            "setValues": [{"language": "de", "value": "Bayern"}],
                        },
                        {
                            "forValues": ["10"],
                            "setValues": [{"language": "de", "value": "Saarland"}],
                        },
                        {
                            "forValues": ["11"],
                            "setValues": [{"language": "de", "value": "Berlin"}],
                        },
                        {
                            "forValues": ["12"],
                            "setValues": [{"language": "de", "value": "Brandenburg"}],
                        },
                        {
                            "forValues": ["13"],
                            "setValues": [
                                {"language": "de", "value": "Mecklenburg-Vorpommern"}
                            ],
                        },
                        {
                            "forValues": ["14"],
                            "setValues": [{"language": "de", "value": "Sachsen"}],
                        },
                        {
                            "forValues": ["15"],
                            "setValues": [
                                {"language": "de", "value": "Sachsen-Anhalt"}
                            ],
                        },
                        {
                            "forValues": ["16"],
                            "setValues": [{"language": "de", "value": "Thüringen"}],
                        },
                    ],
                },
            ],
            "theme": [
                {
                    "comment": "Meldewesen, Infektionskrankheiten",
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {
                            "setValues": [
                                "https://mex.rki.de/item/theme-17",
                                "https://mex.rki.de/item/theme-2",
                            ]
                        }
                    ],
                }
            ],
            "title": [
                {
                    "fieldInPrimarySource": "DiseaseName",
                    "locationInPrimarySource": "Meta.Disease",
                    "mappingRules": [
                        {
                            "rule": "Construct the title after the following "
                            "schema: Meldedaten nach "
                            "Infektionsschutzgesetz (IfSG) zu "
                            "[DiseaseName]. Assign 'de' as default "
                            "for the language property of the text "
                            "object."
                        }
                    ],
                }
            ],
            "unitInCharge": [
                {
                    "fieldInPrimarySource": "n/a",
                    "mappingRules": [
                        {
                            "forValues": ["FG99"],
                            "rule": "Use value to match with "
                            "identifier from "
                            "/raw-data/organigram/organizational-units.json.",
                        }
                    ],
                }
            ],
        },
    ]


@pytest.fixture
def extracted_primary_sources_ifsg() -> ExtractedPrimarySource:
    seed_primary_sources = extract_seed_primary_sources()
    extracted_primary_sources = (
        transform_seed_primary_sources_to_extracted_primary_sources(
            seed_primary_sources
        )
    )

    (extracted_primary_source,) = get_primary_sources_by_name(
        extracted_primary_sources,
        "ifsg",
    )

    return extracted_primary_source


@pytest.fixture
def extracted_ifsg_resource_parent() -> ExtractedResource:
    return ExtractedResource(
        hadPrimarySource="gmbJenBOxZ6AkQAx9Y8nl5",
        identifierInPrimarySource="tbd",
        accessPlatform=[],
        accessRestriction="https://mex.rki.de/item/access-restriction-2",
        accrualPeriodicity="https://mex.rki.de/item/frequency-15",
        alternativeTitle=[Text(value="IfSG Meldedaten", language=TextLanguage.DE)],
        anonymizationPseudonymization=[],
        contact=["bFQoRhcVH5DHU6"],
        contributingUnit=[],
        contributor=[],
        created=None,
        creator=[],
        description=[
            Text(value="Das Infektionsschutzgesetz", language=TextLanguage.DE)
        ],
        distribution=[],
        documentation=[],
        externalPartner=[],
        icd10code=[],
        instrumentToolOrApparatus=[],
        isPartOf=[],
        keyword=[Text(value="Infektionsschutzgesetz", language=TextLanguage.DE)],
        language=["https://mex.rki.de/item/language-1"],
        license=None,
        loincId=[],
        meshId=[],
        method=[],
        methodDescription=[],
        modified=None,
        publication=[
            Link(
                language=LinkLanguage.DE,
                title="Infektionsepidemiologisches Jahrbuch",
                url="https://www.rki.de/DE/Content/Infekt/Jahrbuch/jahrbuch_node.html",
            )
        ],
        publisher=[],
        qualityInformation=[],
        resourceTypeGeneral=["https://mex.rki.de/item/resource-type-general-1"],
        resourceTypeSpecific=[Text(value="Meldedaten", language=TextLanguage.DE)],
        rights=[Text(value="Gesundheitsdaten.", language=TextLanguage.DE)],
        sizeOfDataBasis=None,
        spatial=[Text(value="Deutschland", language=TextLanguage.DE)],
        stateOfDataProcessing=[],
        temporal=None,
        theme=["https://mex.rki.de/item/theme-17"],
        title=[
            Text(
                value="Meldedaten nach Infektionsschutzgesetz (IfSG)",
                language=TextLanguage.DE,
            )
        ],
        unitInCharge=["bFQoRhcVH5DHU7"],
        wasGeneratedBy=None,
    )


@pytest.fixture
def extracted_ifsg_resource_state() -> list[ExtractedResource]:
    return [
        ExtractedResource(
            hadPrimarySource="dU8TU9RAX8EsYCFIde5NUu",
            identifierInPrimarySource="01",
            accessPlatform=[],
            accessRestriction="https://mex.rki.de/item/access-restriction-2",
            accrualPeriodicity="https://mex.rki.de/item/frequency-17",
            alternativeTitle=[
                Text(value="Meldedaten Schleswig-Holstein", language=TextLanguage.DE)
            ],
            anonymizationPseudonymization=[],
            contact=["bFQoRhcVH5DHU6"],
            contributingUnit=[],
            contributor=[],
            created=None,
            creator=[],
            description=[],
            distribution=[],
            documentation=[],
            externalPartner=[],
            icd10code=[],
            instrumentToolOrApparatus=[],
            isPartOf=["eMzHOpNx0evkZAHMle6ZKd"],
            keyword=[Text(value="Infektionsschutzgesetz", language=TextLanguage.DE)],
            language=["https://mex.rki.de/item/language-1"],
            license=None,
            loincId=[],
            meshId=[],
            method=[],
            methodDescription=[],
            modified=None,
            publication=[
                Link(
                    language=LinkLanguage.DE,
                    title="Infektionsepidemiologisches Jahrbuch",
                    url="https://www.rki.de/DE/Content/Infekt/Jahrbuch/jahrbuch_node.html",
                ),
                Link(
                    language=LinkLanguage.DE,
                    title="Epidemiologisches Bulletin",
                    url="https://www.rki.de/DE/Content/Infekt/EpidBull/epid_bull_node.html",
                ),
                Link(
                    language=LinkLanguage.DE,
                    title="Falldefinitionen",
                    url="https://www.rki.de/DE/Content/Infekt/IfSG/Falldefinition/falldefinition_node.html",
                ),
            ],
            publisher=[],
            qualityInformation=[],
            resourceTypeGeneral=["https://mex.rki.de/item/resource-type-general-1"],
            resourceTypeSpecific=[Text(value="Meldedaten", language=TextLanguage.DE)],
            rights=[Text(value="Gesundheitsdaten.", language=TextLanguage.DE)],
            sizeOfDataBasis=None,
            spatial=[Text(value="Schleswig-Holstein", language=TextLanguage.DE)],
            stateOfDataProcessing=[],
            temporal=None,
            theme=["https://mex.rki.de/item/theme-17"],
            title=[Text(value="Schleswig-Holstein", language=TextLanguage.DE)],
            unitInCharge=["bFQoRhcVH5DHU7"],
            wasGeneratedBy=None,
        ),
        ExtractedResource(
            hadPrimarySource="dU8TU9RAX8EsYCFIde5NUu",
            identifierInPrimarySource="02",
            accessPlatform=[],
            accessRestriction="https://mex.rki.de/item/access-restriction-2",
            accrualPeriodicity="https://mex.rki.de/item/frequency-17",
            alternativeTitle=[
                Text(value="Meldedaten Hamburg", language=TextLanguage.DE)
            ],
            anonymizationPseudonymization=[],
            contact=["bFQoRhcVH5DHU6"],
            contributingUnit=[],
            contributor=[],
            created=None,
            creator=[],
            description=[],
            distribution=[],
            documentation=[],
            externalPartner=[],
            icd10code=[],
            instrumentToolOrApparatus=[],
            isPartOf=["eMzHOpNx0evkZAHMle6ZKd"],
            keyword=[Text(value="Infektionsschutzgesetz", language=TextLanguage.DE)],
            language=["https://mex.rki.de/item/language-1"],
            license=None,
            loincId=[],
            meshId=[],
            method=[],
            methodDescription=[],
            modified=None,
            publication=[
                Link(
                    language=LinkLanguage.DE,
                    title="Infektionsepidemiologisches Jahrbuch",
                    url="https://www.rki.de/DE/Content/Infekt/Jahrbuch/jahrbuch_node.html",
                ),
                Link(
                    language=LinkLanguage.DE,
                    title="Epidemiologisches Bulletin",
                    url="https://www.rki.de/DE/Content/Infekt/EpidBull/epid_bull_node.html",
                ),
                Link(
                    language=LinkLanguage.DE,
                    title="Falldefinitionen",
                    url="https://www.rki.de/DE/Content/Infekt/IfSG/Falldefinition/falldefinition_node.html",
                ),
            ],
            publisher=[],
            qualityInformation=[],
            resourceTypeGeneral=["https://mex.rki.de/item/resource-type-general-1"],
            resourceTypeSpecific=[Text(value="Meldedaten", language=TextLanguage.DE)],
            rights=[Text(value="Gesundheitsdaten.", language=TextLanguage.DE)],
            sizeOfDataBasis=None,
            spatial=[Text(value="Hamburg", language=TextLanguage.DE)],
            stateOfDataProcessing=[],
            temporal=None,
            theme=["https://mex.rki.de/item/theme-17"],
            title=[Text(value="Hamburg", language=TextLanguage.DE)],
            unitInCharge=["bFQoRhcVH5DHU7"],
            wasGeneratedBy=None,
        ),
        ExtractedResource(
            hadPrimarySource="dU8TU9RAX8EsYCFIde5NUu",
            identifierInPrimarySource="07",
            accessPlatform=[],
            accessRestriction="https://mex.rki.de/item/access-restriction-2",
            accrualPeriodicity="https://mex.rki.de/item/frequency-17",
            alternativeTitle=[
                Text(value="Meldedaten Schleswig-Holstein", language=TextLanguage.DE)
            ],
            anonymizationPseudonymization=[],
            contact=["bFQoRhcVH5DHU6"],
            contributingUnit=[],
            contributor=[],
            created=None,
            creator=[],
            description=[],
            distribution=[],
            documentation=[],
            externalPartner=[],
            icd10code=[],
            instrumentToolOrApparatus=[],
            isPartOf=["eMzHOpNx0evkZAHMle6ZKd"],
            keyword=[Text(value="Infektionsschutzgesetz", language=TextLanguage.DE)],
            language=["https://mex.rki.de/item/language-1"],
            license=None,
            loincId=[],
            meshId=[],
            method=[],
            methodDescription=[],
            modified=None,
            publication=[
                Link(
                    language=LinkLanguage.DE,
                    title="Infektionsepidemiologisches Jahrbuch",
                    url="https://www.rki.de/DE/Content/Infekt/Jahrbuch/jahrbuch_node.html",
                ),
                Link(
                    language=LinkLanguage.DE,
                    title="Epidemiologisches Bulletin",
                    url="https://www.rki.de/DE/Content/Infekt/EpidBull/epid_bull_node.html",
                ),
                Link(
                    language=LinkLanguage.DE,
                    title="Falldefinitionen",
                    url="https://www.rki.de/DE/Content/Infekt/IfSG/Falldefinition/falldefinition_node.html",
                ),
            ],
            publisher=[],
            qualityInformation=[],
            resourceTypeGeneral=["https://mex.rki.de/item/resource-type-general-1"],
            resourceTypeSpecific=[Text(value="Meldedaten", language=TextLanguage.DE)],
            rights=[Text(value="Gesundheitsdaten.", language=TextLanguage.DE)],
            sizeOfDataBasis=None,
            spatial=[Text(value="Schleswig-Holstein", language=TextLanguage.DE)],
            stateOfDataProcessing=[],
            temporal=None,
            theme=["https://mex.rki.de/item/theme-17"],
            title=[Text(value="Schleswig-Holstein", language=TextLanguage.DE)],
            unitInCharge=["bFQoRhcVH5DHU7"],
            wasGeneratedBy=None,
        ),
        ExtractedResource(
            hadPrimarySource="dU8TU9RAX8EsYCFIde5NUu",
            identifierInPrimarySource="09",
            accessPlatform=[],
            accessRestriction="https://mex.rki.de/item/access-restriction-2",
            accrualPeriodicity="https://mex.rki.de/item/frequency-17",
            alternativeTitle=[
                Text(value="Meldedaten Hamburg", language=TextLanguage.DE)
            ],
            anonymizationPseudonymization=[],
            contact=["bFQoRhcVH5DHU6"],
            contributingUnit=[],
            contributor=[],
            created=None,
            creator=[],
            description=[],
            distribution=[],
            documentation=[],
            externalPartner=[],
            icd10code=[],
            instrumentToolOrApparatus=[],
            isPartOf=["eMzHOpNx0evkZAHMle6ZKd"],
            keyword=[Text(value="Infektionsschutzgesetz", language=TextLanguage.DE)],
            language=["https://mex.rki.de/item/language-1"],
            license=None,
            loincId=[],
            meshId=[],
            method=[],
            methodDescription=[],
            modified=None,
            publication=[
                Link(
                    language=LinkLanguage.DE,
                    title="Infektionsepidemiologisches Jahrbuch",
                    url="https://www.rki.de/DE/Content/Infekt/Jahrbuch/jahrbuch_node.html",
                ),
                Link(
                    language=LinkLanguage.DE,
                    title="Epidemiologisches Bulletin",
                    url="https://www.rki.de/DE/Content/Infekt/EpidBull/epid_bull_node.html",
                ),
                Link(
                    language=LinkLanguage.DE,
                    title="Falldefinitionen",
                    url="https://www.rki.de/DE/Content/Infekt/IfSG/Falldefinition/falldefinition_node.html",
                ),
            ],
            publisher=[],
            qualityInformation=[],
            resourceTypeGeneral=["https://mex.rki.de/item/resource-type-general-1"],
            resourceTypeSpecific=[Text(value="Meldedaten", language=TextLanguage.DE)],
            rights=[Text(value="Gesundheitsdaten.", language=TextLanguage.DE)],
            sizeOfDataBasis=None,
            spatial=[Text(value="Hamburg", language=TextLanguage.DE)],
            stateOfDataProcessing=[],
            temporal=None,
            theme=["https://mex.rki.de/item/theme-17"],
            title=[Text(value="Hamburg", language=TextLanguage.DE)],
            unitInCharge=["bFQoRhcVH5DHU7"],
            wasGeneratedBy=None,
        ),
    ]


@pytest.fixture
def extracted_ifsg_resource_disease() -> list[ExtractedResource]:
    return [
        ExtractedResource(
            hadPrimarySource="fU5a2ZiWXu9ItMX7gYuuPv",
            identifierInPrimarySource="101",
            accessPlatform=[],
            accessRestriction="https://mex.rki.de/item/access-restriction-2",
            accrualPeriodicity="https://mex.rki.de/item/frequency-17",
            alternativeTitle=[Text(value="ABC", language=None)],
            anonymizationPseudonymization=[],
            contact=["bFQoRhcVH5DHU6"],
            contributingUnit=[],
            contributor=[],
            created=None,
            creator=[],
            description=[],
            distribution=[],
            documentation=[],
            externalPartner=[],
            icd10code=["A1"],
            instrumentToolOrApparatus=[
                Text(value="Falldefinition B", language=TextLanguage.DE),
                Text(value="Falldefinition C", language=TextLanguage.DE),
            ],
            isPartOf=[
                "hWaaedrfn2ammVuBSZL4TD",
                "dwbN9TmQwDrEp6a0qriDIf",
                "dEefZZfVSd6l9Lj8JZqjg",
            ],
            keyword=[
                Text(value="virus", language=None),
                Text(value="Epidemic", language=None),
                Text(value="virus", language=None),
            ],
            language=["https://mex.rki.de/item/language-1"],
            license=None,
            loincId=[],
            meshId=[],
            method=[],
            methodDescription=[],
            modified=None,
            publication=[
                Link(
                    language=LinkLanguage.DE,
                    title="Falldefinitionen",
                    url="https://www.rki.de/DE/Content/Infekt/IfSG/Falldefinition/falldefinition_node.html",
                )
            ],
            publisher=[],
            qualityInformation=[],
            resourceTypeGeneral=["https://mex.rki.de/item/resource-type-general-1"],
            resourceTypeSpecific=[Text(value="Meldedaten", language=TextLanguage.DE)],
            rights=[Text(value="Gesundheitsdaten.", language=TextLanguage.DE)],
            sizeOfDataBasis=None,
            spatial=[Text(value="Deutschland", language=TextLanguage.DE)],
            stateOfDataProcessing=[],
            temporal=None,
            theme=[
                "https://mex.rki.de/item/theme-17",
                "https://mex.rki.de/item/theme-2",
            ],
            title=[Text(value="virus", language=None)],
            unitInCharge=["bFQoRhcVH5DHU7"],
            wasGeneratedBy=None,
        )
    ]


@pytest.fixture
def extracted_ifsg_variable_group() -> list[ExtractedVariableGroup]:
    return [
        ExtractedVariableGroup(
            hadPrimarySource=MergedPrimarySourceIdentifier.generate(23),
            identifierInPrimarySource="101_Epi",
            containedBy=[MergedResourceIdentifier.generate(24)],
            label=[
                Text(value="Epidemiologische Informationen", language=TextLanguage.DE)
            ],
        )
    ]
