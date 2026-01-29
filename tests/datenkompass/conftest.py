from collections.abc import Generator
from typing import cast
from unittest.mock import MagicMock, patch

import pytest
from pytest import MonkeyPatch

from mex.common.backend_api.connector import BackendApiConnector
from mex.common.identity import Identity, get_provider
from mex.common.models import (
    AnyMergedModel,
    MergedActivity,
    MergedBibliographicResource,
    MergedContactPoint,
    MergedOrganization,
    MergedOrganizationalUnit,
    MergedPerson,
    MergedPrimarySource,
    MergedResource,
)
from mex.common.types import (
    AccessRestriction,
    Link,
    MergedActivityIdentifier,
    MergedBibliographicResourceIdentifier,
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
    Text,
    TextLanguage,
)
from mex.extractors.datenkompass.models.item import DatenkompassActivity
from mex.extractors.datenkompass.models.mapping import DatenkompassMapping
from mex.extractors.settings import Settings
from mex.extractors.utils import load_yaml


@pytest.fixture
def mocked_activity_mapping() -> DatenkompassMapping:
    settings = Settings.get()
    return DatenkompassMapping.model_validate(
        load_yaml(settings.datenkompass.mapping_path / "activity.yaml")
    )


@pytest.fixture
def mocked_bibliographic_resource_mapping() -> DatenkompassMapping:
    settings = Settings.get()
    return DatenkompassMapping.model_validate(
        load_yaml(settings.datenkompass.mapping_path / "bibliographic-resource.yaml")
    )


@pytest.fixture
def mocked_resource_mapping() -> DatenkompassMapping:
    settings = Settings.get()
    return DatenkompassMapping.model_validate(
        load_yaml(settings.datenkompass.mapping_path / "resource.yaml")
    )


@pytest.fixture
def mocked_merged_activities() -> list[MergedActivity]:
    """Mock a list of Merged Activity items."""
    return [
        MergedActivity(
            contact=["LoremIpsum1234"],
            responsibleUnit=["IdentifierUnitC1", "IdentifierUnitFG99"],
            title=[
                Text(value='"title "Act" no language"', language=None),
                Text(value="title en", language="en"),
            ],
            abstract=[
                Text(value="Die Nutzung", language="de"),
                Text(value="The usage", language="en"),
            ],
            end=["2025-02-25"],
            funderOrCommissioner=[
                MergedOrganizationIdentifier("Identifier2forORG"),
                MergedOrganizationIdentifier("NoORGIdentifier"),
            ],
            shortName=[
                Text(value="short en", language="en"),
                Text(value="short de", language="de"),
            ],
            start=["2021-02-21"],
            theme=["https://mex.rki.de/item/theme-11"],  # INFECTIOUS_DISEASES_AND_...
            website=[
                Link(language=None, url="https://www.dont-transform.de"),
                Link(language="de", title="Ja", url="https://www.do-transform.org"),
            ],
            entityType="MergedActivity",
            identifier=MergedActivityIdentifier("MergedActivityWithORG2"),
        ),
        MergedActivity(
            contact=["LoremIpsum3456"],
            responsibleUnit=["IdentifierUnitPRNT"],
            title=[
                Text(value="titel de", language="de"),
                Text(value="title en", language="en"),
            ],
            abstract=[Text(value="Without language", language=None)],
            end=["1970-01-01"],
            funderOrCommissioner=[MergedOrganizationIdentifier("Identifier1forORG")],
            shortName=[Text(value="short only english", language="en")],
            theme=["https://mex.rki.de/item/theme-11"],  # INFECTIOUS_DISEASES_AND_ ...
            entityType="MergedActivity",
            identifier=MergedActivityIdentifier("MergedActivityWithORG1"),
        ),
        MergedActivity(
            contact=["LoremIpsum5678"],
            responsibleUnit=["IdentifierUnitFG99"],
            title=[Text(value="should get filtered out", language="en")],
            funderOrCommissioner=[MergedOrganizationIdentifier("NoORGIdentifier")],
            theme=["https://mex.rki.de/item/theme-11"],  # INFECTIOUS_DISEASES_AND_ ..
            entityType="MergedActivity",
            identifier=MergedActivityIdentifier("MergedActivityNoORG"),
        ),
    ]


@pytest.fixture
def mocked_merged_bibliographic_resource() -> list[MergedBibliographicResource]:
    """Mock a list of Merged Bibliographic Resource items."""
    return [
        MergedBibliographicResource(
            accessRestriction=AccessRestriction["OPEN"],
            title=[
                Text(value='title "BibRes" no language', language=None),
                Text(value="title en", language="en"),
            ],
            abstract=[
                Text(value="Die Nutzung", language="de"),
                Text(value="The usage", language="en"),
            ],
            contributingUnit=[MergedOrganizationalUnitIdentifier("IdentifierUnitPRNT")],
            keyword=[
                Text(value="short en", language="en"),
                Text(value="short de", language="de"),
            ],
            doi="https://doi.org/10.1234_find_this",
            alternateIdentifier=["find_second_a", "find_second_b"],
            repositoryURL=["https://www.ignore_this.to"],
            bibliographicResourceType=[
                "https://mex.rki.de/item/bibliographic-resource-type-1"
            ],  # BOOK
            creator=["PersonIdentifier4Peppa"] * 6,
            entityType="MergedBibliographicResource",
            identifier=MergedBibliographicResourceIdentifier("MergedBibResource1"),
        ),
    ]


@pytest.fixture
def mocked_merged_resource() -> list[MergedResource]:
    """Mock a list of Merged Resource items."""
    return [
        MergedResource(
            accessRestriction=AccessRestriction["OPEN"],
            description=[
                Text(value="english description", language="en"),
                Text(
                    value=("deutsche Beschreibung <a href='http://mit.link'>skip</a>."),
                    language="de",
                ),
            ],
            contact=[
                "PersonIdentifier4Peppa",
                "IdentifierUnitC1",
                "IdentifierUnitFG99",
                "identifier4contactPt",
            ],
            doi="https://doi.org/10.1234_example",
            hasLegalBasis=[
                Text(value="has basis", language="en"),
                Text(value="hat weitere Basis", language="de"),
            ],
            hasPurpose=[Text(value="has purpose", language=None)],
            keyword=[
                Text(value="word 1", language="en"),
                Text(value="Wort 2", language="de"),
            ],
            theme=["https://mex.rki.de/item/theme-11"],  # INFECTIOUS_DISEASES_AND_...
            title=["some Source-2 resource title"],
            wasGeneratedBy=["MergedActivityWithORG2"],
            unitInCharge=["IdentifierUnitPRNT"],
            identifier=["Source2Resource"],
        ),
        MergedResource(
            accessRestriction=AccessRestriction["RESTRICTED"],
            contact=["PersonIdentifier4Peppa"],
            theme=["https://mex.rki.de/item/theme-11"],  # INFECTIOUS_DISEASES_AND_...
            title=["some Source-1 resource title"],
            wasGeneratedBy=["MergedActivityNoORG"],
            unitInCharge=["IdentifierUnitC1"],
            identifier=["Source1Resource"],
        ),
    ]


@pytest.fixture
def mocked_merged_organizational_units() -> list[MergedOrganizationalUnit]:
    """Mock a list of Merged Organizational Unit items."""
    return [
        MergedOrganizationalUnit(
            parentUnit="IdentifierUnitPRNT",
            name=[
                Text(value="CHLD Unterabteilung", language=TextLanguage.DE),
                Text(value="C1: Sub Unit", language=TextLanguage.EN),
            ],
            alternativeName=[
                Text(value="CHLD", language=TextLanguage.EN),
                Text(value="C1 Sub-Unit", language=TextLanguage.EN),
                Text(value="C1 Unterabteilung", language=TextLanguage.DE),
            ],
            email=[],
            shortName=[Text(value="C1", language=TextLanguage.DE)],
            entityType="MergedOrganizationalUnit",
            identifier="IdentifierUnitC1",
        ),
        MergedOrganizationalUnit(
            parentUnit=None,
            name=[
                Text(value="Abteilung", language=TextLanguage.DE),
                Text(value="Department", language=TextLanguage.EN),
            ],
            alternativeName=[
                Text(value="PRNT Abteilung", language=TextLanguage.DE),
                Text(value="PARENT Dept.", language=None),
            ],
            email=["pu@example.com", "PARENT@example.com"],
            shortName=[Text(value="PRNT", language=TextLanguage.DE)],
            entityType="MergedOrganizationalUnit",
            identifier="IdentifierUnitPRNT",
        ),
        MergedOrganizationalUnit(
            parentUnit=None,
            name=[
                Text(value="Fachgebiet 99", language=TextLanguage.DE),
                Text(value="Group 99", language=TextLanguage.EN),
            ],
            alternativeName=[Text(value="FG99", language=TextLanguage.DE)],
            email=["fg@example.com"],
            shortName=[Text(value="FG 99", language=TextLanguage.DE)],
            entityType="MergedOrganizationalUnit",
            identifier="IdentifierUnitFG99",
        ),
    ]


@pytest.fixture
def mocked_merged_organization() -> list[MergedOrganization]:
    """Mock a list of organizations as Merged Organization items."""
    return [
        MergedOrganization(
            officialName=[Text(value="Organization 1", language="de")],
            entityType="MergedOrganization",
            identifier=MergedOrganizationIdentifier("Identifier1forORG"),
        ),
        MergedOrganization(
            officialName=[Text(value="Organization 2", language=None)],
            entityType="MergedOrganization",
            identifier=MergedOrganizationIdentifier("Identifier2forORG"),
        ),
    ]


@pytest.fixture
def mocked_merged_person() -> list[MergedPerson]:
    """Mock a single Merged Person item."""
    return [
        MergedPerson(
            fullName=["Pattern, Peppa P.", "Pattern, P.P."],
            email=["PatternPP@example.org"],
            entityType="MergedPerson",
            identifier=MergedPersonIdentifier("PersonIdentifier4Peppa"),
        )
    ]


@pytest.fixture
def mocked_merged_contact_point() -> list[MergedContactPoint]:
    """Mock a list of Merged Contact Point items."""
    return [
        MergedContactPoint(
            email=["contactpoint@example.org"],
            identifier=[MergedContactPointIdentifier("identifier4contactPt")],
        ),
    ]


@pytest.fixture
def mocked_merged_primary_sources() -> list[MergedPrimarySource]:
    """Mock a list of merged Primary Source items."""
    return [
        MergedPrimarySource(
            entityType="MergedPrimarySource",
            identifier="SomeIrrelevantPS",
        ),
        MergedPrimarySource(
            title=[Text(value="this is a Relevant Primary Source", language="en")],
            entityType="MergedPrimarySource",
            identifier="identifierRelevantPS",
        ),
    ]


@pytest.fixture
def mocked_datenkompass_activity() -> list[DatenkompassActivity]:
    """Mock a list of Datenkompass Activity items."""
    return [
        DatenkompassActivity(
            beschreibung="Es handelt. Die Nutzung",
            datenhalter="Datenhalter",
            kontakt="fg@example.com",
            organisationseinheit="C1; FG 99",
            titel="short de; title 'Act' no language; title en",
            schlagwort="Infektionskrankheiten und -epidemiologie",
            datenbank="https://www.do-transform.org",
            zeitliche_abdeckung="2021-02-21 - 2025-02-25",
            voraussetzungen="Voraussetzungen",
            frequenz="Frequenz",
            hauptkategorie="Hauptkategorie",
            unterkategorie="Unterkategorie",
            rechtsgrundlage="Rechtsgrundlage",
            datenerhalt="Datenerhalt",
            status="Status",
            datennutzungszweck="Datennutzungszweck",
            herausgeber="Herausgeber",
            kommentar="Kommentar",
            dk_format="Format",
            identifier="MergedActivityWithORG2",
            entityType="MergedActivity",
        ),
        DatenkompassActivity(
            beschreibung="Es handelt. Without language",
            datenhalter="Datenhalter",
            kontakt="pu@example.com",
            organisationseinheit="PRNT",
            titel="short only english; titel de",
            schlagwort="Infektionskrankheiten und -epidemiologie",
            datenbank=None,
            zeitliche_abdeckung="1970-01-01",
            voraussetzungen="Voraussetzungen",
            frequenz="Frequenz",
            hauptkategorie="Hauptkategorie",
            unterkategorie="Unterkategorie",
            rechtsgrundlage="Rechtsgrundlage",
            datenerhalt="Datenerhalt",
            status="Status",
            datennutzungszweck="Datennutzungszweck",
            herausgeber="Herausgeber",
            kommentar="Kommentar",
            dk_format="Format",
            identifier="MergedActivityWithORG1",
            entityType="MergedActivity",
        ),
    ]


@pytest.fixture
def mocked_backend_datenkompass(  # noqa: PLR0913
    monkeypatch: MonkeyPatch,
    mocked_merged_activities: list[MergedActivity],
    mocked_merged_bibliographic_resource: list[MergedBibliographicResource],
    mocked_merged_resource: list[MergedResource],
    mocked_merged_organizational_units: list[MergedOrganizationalUnit],
    mocked_merged_organization: list[MergedOrganization],
    mocked_merged_person: list[MergedPerson],
    mocked_merged_contact_point: list[MergedContactPoint],
    mocked_merged_primary_sources: list[MergedPrimarySource],
) -> MagicMock:
    """Mock the backendAPIConnector functions to return dummy variables."""
    mock_dispatch = {
        "MergedActivity": mocked_merged_activities,
        "MergedBibliographicResource": mocked_merged_bibliographic_resource,
        "MergedResource": mocked_merged_resource,
        "MergedPrimarySource": mocked_merged_primary_sources,
        "MergedOrganizationalUnit": mocked_merged_organizational_units,
        "MergedOrganization": mocked_merged_organization,
        "MergedPerson": mocked_merged_person,
        "MergedContactPoint": mocked_merged_contact_point,
    }

    def fetch_all_merged_items(
        *,
        query_string: str | None = None,  # noqa: ARG001
        entity_type: list[str] | None = None,
        referenced_identifier: list[str] | None = None,  # noqa: ARG001
        reference_field: str | None = None,  # noqa: ARG001
    ) -> list[AnyMergedModel]:
        if entity_type and len(entity_type) > 0:
            key = entity_type[0]
        else:
            pytest.fail("No entity_type given in query to Backend.")

        return cast("list[AnyMergedModel]", mock_dispatch.get(key))

    backend = MagicMock(
        fetch_all_merged_items=MagicMock(
            spec=BackendApiConnector.fetch_all_merged_items,
            side_effect=fetch_all_merged_items,
        ),
    )
    monkeypatch.setattr(
        BackendApiConnector, "_check_availability", MagicMock(return_value=True)
    )
    monkeypatch.setattr(
        BackendApiConnector, "fetch_all_merged_items", backend.fetch_all_merged_items
    )
    return backend


@pytest.fixture
def mocked_provider(monkeypatch: MonkeyPatch) -> MagicMock:
    """Mock the IdentityProvider functions to return dummy variables."""

    def fetch(stable_target_id: str) -> list[Identity]:
        if stable_target_id == "SomeIrrelevantPS":
            return [
                Identity(
                    identifier="12345678901234",
                    hadPrimarySource="00000000000000",
                    identifierInPrimarySource="completely irrelevant",
                    stableTargetId="SomeIrrelevantPS",
                )
            ]
        if stable_target_id == "identifierRelevantPS":
            return [
                Identity(
                    identifier="98765432109876",
                    hadPrimarySource="00000000000000",
                    identifierInPrimarySource="relevant primary source",
                    stableTargetId="identifierRelevantPS",
                )
            ]
        pytest.fail("wrong mocking of identity provider")

    provider = get_provider()

    fake_provider = MagicMock(
        fetch=MagicMock(spec=provider.fetch, side_effect=fetch),
    )

    monkeypatch.setattr(provider, "fetch", fake_provider.fetch)

    return fake_provider


@pytest.fixture  # needed for hardcoded upload to S3.
def mocked_boto() -> Generator[MagicMock]:
    """Mock a S3 session client to write the jsons to."""
    with patch("boto3.Session") as mock_session_class:
        mock_s3_client = MagicMock()
        mock_session_instance = MagicMock()
        mock_session_instance.client.return_value = mock_s3_client
        mock_session_class.return_value = mock_session_instance

        yield mock_s3_client
