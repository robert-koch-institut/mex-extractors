from typing import Any
from uuid import UUID

import pytest

from mex.common.ldap.models.person import LDAPPerson, LDAPPersonWithQuery
from mex.common.models import (
    ExtractedAccessPlatform,
    ExtractedActivity,
    ExtractedPerson,
    ExtractedPrimarySource,
)
from mex.common.primary_source.extract import extract_seed_primary_sources
from mex.common.primary_source.transform import (
    get_primary_sources_by_name,
    transform_seed_primary_sources_to_extracted_primary_sources,
)
from mex.common.types import (
    Identifier,
    MergedOrganizationalUnitIdentifier,
    MergedPersonIdentifier,
)
from mex.extractors.seq_repo.filter import filter_sources_on_latest_sequencing_date
from mex.extractors.seq_repo.model import SeqRepoSource
from mex.extractors.seq_repo.transform import (
    transform_seq_repo_access_platform_to_extracted_access_platform,
    transform_seq_repo_activities_to_extracted_activities,
)


@pytest.fixture(autouse=True)
def extracted_primary_source_seq_repo() -> ExtractedPrimarySource:
    seed_primary_sources = extract_seed_primary_sources()
    extracted_primary_sources = (
        transform_seed_primary_sources_to_extracted_primary_sources(
            seed_primary_sources
        )
    )

    (extracted_primary_source_seq_repo,) = get_primary_sources_by_name(
        extracted_primary_sources,
        "seq-repo",
    )

    return extracted_primary_source_seq_repo


@pytest.fixture
def seq_repo_sources() -> list[SeqRepoSource]:
    return [
        SeqRepoSource(
            project_coordinators=["max", "mustermann", "yee-haw"],
            customer_org_unit_id="FG99",
            sequencing_date="2023-08-07",
            lims_sample_id="test-sample-id",
            sequencing_platform="TEST",
            species="Severe acute respiratory syndrome coronavirus 2",
            project_name="FG99-ABC-123",
            customer_sample_name="test-customer-name-1",
            project_id="TEST-ID",
        ),
        SeqRepoSource(
            project_coordinators=["jelly", "fish", "turtle"],
            customer_org_unit_id="FG99",
            sequencing_date="2023-08-07",
            lims_sample_id="test-sample-id",
            sequencing_platform="TEST",
            species="Lab rat",
            project_name="FG99-ABC-321",
            customer_sample_name="test-customer-name-2",
            project_id="TEST-ID",
        ),
    ]


@pytest.fixture
def seq_repo_latest_sources(
    seq_repo_sources: list[SeqRepoSource],
) -> dict[str, SeqRepoSource]:
    return filter_sources_on_latest_sequencing_date(seq_repo_sources)


@pytest.fixture
def seq_repo_activity() -> dict[str, Any]:
    return {
        "theme": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {
                        "setValues": [
                            "https://mex.rki.de/item/theme-11",
                            "https://mex.rki.de/item/theme-23",
                        ]
                    }
                ],
            }
        ],
    }


@pytest.fixture
def seq_repo_access_platform() -> dict[str, Any]:
    return {
        "identifierInPrimarySource": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {
                        "setValues": ["https://dummy.url.com/"],
                    }
                ],
            }
        ],
        "alternativeTitle": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {
                        "setValues": [{"value": "SeqRepo", "language": None}],
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
                            {
                                "value": "This is just a sample description, don't read it.",
                                "language": "en",
                            }
                        ],
                    }
                ],
            }
        ],
        "endpointType": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [{"setValues": ["https://mex.rki.de/item/api-type-1"]}],
            }
        ],
        "landingPage": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {
                        "setValues": [
                            {
                                "language": None,
                                "title": None,
                                "url": "https://dummy.url.com/",
                            }
                        ],
                    }
                ],
            }
        ],
        "technicalAccessibility": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {"setValues": ["https://mex.rki.de/item/technical-accessibility-1"]}
                ],
            }
        ],
        "title": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {
                        "setValues": [
                            {"value": "Sequence Data Repository", "language": None}
                        ],
                    }
                ],
            }
        ],
        "contact": [
            {
                "mappingRules": [
                    {
                        "forValues": ["FG99"],
                        "setValues": None,
                    }
                ],
            }
        ],
    }


@pytest.fixture
def seq_repo_resource() -> dict[str, Any]:
    return {
        "accessRestriction": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {"setValues": ["https://mex.rki.de/item/access-restriction-2"]}
                ],
            }
        ],
        "accrualPeriodicity": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {"setValues": ["https://mex.rki.de/item/frequency-15"]}
                ],
            }
        ],
        "anonymizationPseudonymization": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {
                        "setValues": [
                            "https://mex.rki.de/item/anonymization-pseudonymization-2"
                        ]
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
                            {"value": "fastc", "language": "de"},
                            {"value": "fastd", "language": "de"},
                        ],
                    }
                ],
            }
        ],
        "method": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {
                        "setValues": [
                            {"value": "Next-Generation Sequencing", "language": "de"},
                            {"value": "NGS", "language": "de"},
                        ],
                    }
                ],
            }
        ],
        "publisher": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {
                        "forValues": ["Robert Koch-Institut"],
                        "setValues": None,
                    }
                ],
            }
        ],
        "resourceCreationMethod": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {
                        "setValues": [
                            "https://mex.rki.de/item/resource-creation-method-4"
                        ]
                    }
                ],
            }
        ],
        "resourceTypeGeneral": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {"setValues": ["https://mex.rki.de/item/resource-type-general-13"]}
                ],
            }
        ],
        "resourceTypeSpecific": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {
                        "setValues": [
                            {"value": "Sequencing Data", "language": "de"},
                            {"value": "Sequenzdaten", "language": "de"},
                        ],
                    }
                ],
            }
        ],
        "rights": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {
                        "setValues": [
                            {
                                "value": "Example content",
                                "language": "de",
                            }
                        ],
                    }
                ],
            }
        ],
        "stateOfDataProcessing": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {"setValues": ["https://mex.rki.de/item/data-processing-state-1"]}
                ],
            }
        ],
        "theme": [
            {
                "fieldInPrimarySource": "n/a",
                "mappingRules": [
                    {
                        "setValues": [
                            "https://mex.rki.de/item/theme-11",
                            "https://mex.rki.de/item/theme-23",
                        ]
                    }
                ],
            }
        ],
    }


@pytest.fixture
def extracted_mex_access_platform(
    extracted_primary_source_seq_repo: ExtractedPrimarySource,
    seq_repo_access_platform: dict[str, Any],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
) -> ExtractedAccessPlatform:
    return transform_seq_repo_access_platform_to_extracted_access_platform(
        seq_repo_access_platform,
        unit_stable_target_ids_by_synonym,
        extracted_primary_source_seq_repo,
    )


@pytest.fixture
def extracted_mex_activities_dict(
    extracted_primary_source_seq_repo: ExtractedPrimarySource,
    seq_repo_latest_sources: dict[str, SeqRepoSource],
    seq_repo_activity: dict[str, Any],
    seq_repo_source_resolved_project_coordinators: list[LDAPPersonWithQuery],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    project_coordinators_merged_ids_by_query_string: dict[
        str, list[MergedPersonIdentifier]
    ],
) -> dict[str, ExtractedActivity]:
    extracted_mex_activities = transform_seq_repo_activities_to_extracted_activities(
        seq_repo_latest_sources,
        seq_repo_activity,
        seq_repo_source_resolved_project_coordinators,
        unit_stable_target_ids_by_synonym,
        project_coordinators_merged_ids_by_query_string,
        extracted_primary_source_seq_repo,
    )
    return {
        activity.identifierInPrimarySource: activity
        for activity in extracted_mex_activities
    }


@pytest.fixture
def seq_repo_source_resolved_project_coordinators() -> list[LDAPPersonWithQuery]:
    """Extract source project coordinators."""
    return [
        LDAPPersonWithQuery(
            person=LDAPPerson(
                sAMAccountName="max",
                objectGUID=UUID("00000000-0000-4000-8000-000000000004"),
                mail=[],
                company=None,
                department="FG99",
                departmentNumber="FG99",
                displayName="mustermann, max",
                employeeID="42",
                givenName=["max"],
                ou=[],
                sn="mustermann",
            ),
            query="max",
        ),
        LDAPPersonWithQuery(
            person=LDAPPerson(
                sAMAccountName="max",
                objectGUID=UUID("00000000-0000-4000-8000-000000000004"),
                mail=[],
                company=None,
                department="FG99",
                departmentNumber="FG99",
                displayName="mustermann, max",
                employeeID="42",
                givenName=["max"],
                ou=[],
                sn="mustermann",
            ),
            query="mustermann",
        ),
    ]


@pytest.fixture
def project_coordinators_merged_ids_by_query_string() -> (
    dict[str, list[MergedPersonIdentifier]]
):
    """Get project coordinators merged ids."""
    return {
        "mustermann": [MergedPersonIdentifier("e0Rxxm9WvnMqPLZ44UduNx")],
        "max": [MergedPersonIdentifier("d6Lni0XPiEQM5jILEBOYxO")],
        "jelly": [MergedPersonIdentifier("buTvstFluFUX9TeoHlhe7c")],
        "fish": [MergedPersonIdentifier("gOwHDDA0HQgT1eDYnC4Ai5")],
    }


@pytest.fixture
def unit_stable_target_ids_by_synonym() -> (
    dict[str, MergedOrganizationalUnitIdentifier]
):
    """Extract the dummy units and return them grouped by synonyms."""
    return {
        "child-unit": MergedOrganizationalUnitIdentifier("g2AinFG4E6n8H1ZMuaBW6o"),
        "CHLD Unterabteilung": MergedOrganizationalUnitIdentifier(
            "g2AinFG4E6n8H1ZMuaBW6o"
        ),
        "C1: Sub Unit": MergedOrganizationalUnitIdentifier("g2AinFG4E6n8H1ZMuaBW6o"),
        "C1": MergedOrganizationalUnitIdentifier("g2AinFG4E6n8H1ZMuaBW6o"),
        "CHLD": MergedOrganizationalUnitIdentifier("g2AinFG4E6n8H1ZMuaBW6o"),
        "C1 Sub-Unit": MergedOrganizationalUnitIdentifier("g2AinFG4E6n8H1ZMuaBW6o"),
        "C1 Unterabteilung": MergedOrganizationalUnitIdentifier(
            "g2AinFG4E6n8H1ZMuaBW6o"
        ),
        "parent-unit": MergedOrganizationalUnitIdentifier("dLqCAZCHhjZmJcJR98ytzQ"),
        "Abteilung": MergedOrganizationalUnitIdentifier("dLqCAZCHhjZmJcJR98ytzQ"),
        "Department": MergedOrganizationalUnitIdentifier("dLqCAZCHhjZmJcJR98ytzQ"),
        "PRNT": MergedOrganizationalUnitIdentifier("dLqCAZCHhjZmJcJR98ytzQ"),
        "PRNT Abteilung": MergedOrganizationalUnitIdentifier("dLqCAZCHhjZmJcJR98ytzQ"),
        "PARENT Dept.": MergedOrganizationalUnitIdentifier("dLqCAZCHhjZmJcJR98ytzQ"),
        "fg99": MergedOrganizationalUnitIdentifier("e4fyMCGjCeQNSvAMNHcBhK"),
        "Fachgebiet 99": MergedOrganizationalUnitIdentifier("e4fyMCGjCeQNSvAMNHcBhK"),
        "Group 99": MergedOrganizationalUnitIdentifier("e4fyMCGjCeQNSvAMNHcBhK"),
        "FG 99": MergedOrganizationalUnitIdentifier("e4fyMCGjCeQNSvAMNHcBhK"),
        "FG99": MergedOrganizationalUnitIdentifier("e4fyMCGjCeQNSvAMNHcBhK"),
    }


@pytest.fixture
def extracted_person() -> ExtractedPerson:
    """Return an extracted person with static dummy values."""
    return ExtractedPerson(
        email=["fictitiousf@rki.de", "info@rki.de"],
        familyName="Fictitious",
        givenName="Frieda",
        fullName="Dr. Fictitious, Frieda",
        identifierInPrimarySource="frieda",
        hadPrimarySource=Identifier.generate(seed=40),
    )
