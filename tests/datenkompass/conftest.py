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
)
from mex.extractors.datenkompass.models.item import DatenkompassActivity


@pytest.fixture
def mocked_merged_activities() -> list[MergedActivity]:
    """Mock a list of Merged Activity items."""
    return [
        MergedActivity(
            contact=["LoremIpsum1234"],
            responsibleUnit=[
                MergedOrganizationalUnitIdentifier("IdentifierOrgUnitZB"),
                MergedOrganizationalUnitIdentifier("IdentifierOrgUnitEG"),
            ],
            title=[
                Text(value='"title "Act" no language"', language=None),
                Text(value="title en", language="en"),
            ],
            abstract=[
                Text(value="Die Nutzung", language="de"),
                Text(value="The usage", language="en"),
            ],
            funderOrCommissioner=[
                MergedOrganizationIdentifier("Identifier2forORG"),
                MergedOrganizationIdentifier("NoORGIdentifier"),
            ],
            shortName=[
                Text(value="short en", language="en"),
                Text(value="short de", language="de"),
            ],
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
            responsibleUnit=[MergedOrganizationalUnitIdentifier("IdentifierOrgUnitEG")],
            title=[
                Text(value="titel de", language="de"),
                Text(value="title en", language="en"),
            ],
            abstract=[Text(value="Without language", language=None)],
            funderOrCommissioner=[MergedOrganizationIdentifier("Identifier1forORG")],
            shortName=[Text(value="short only english", language="en")],
            theme=["https://mex.rki.de/item/theme-11"],  # INFECTIOUS_DISEASES_AND_ ...
            entityType="MergedActivity",
            identifier=MergedActivityIdentifier("MergedActivityWithORG1"),
        ),
        MergedActivity(
            contact=["LoremIpsum5678"],
            responsibleUnit=["DolorSitAmetConsec"],
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
            contributingUnit=[
                MergedOrganizationalUnitIdentifier("IdentifierOrgUnitEG")
            ],
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
                Text(value="deutsche Beschreibung", language="de"),
            ],
            contact=[
                "PersonIdentifier4Peppa",
                "IdentifierOrgUnitEG",
                "IdentifierOrgUnitZB",
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
            title=["some open data resource title"],
            wasGeneratedBy=["MergedActivityWithORG2"],
            unitInCharge=["IdentifierOrgUnitEG"],
            identifier=["openDataResource"],
        ),
        MergedResource(
            accessRestriction=AccessRestriction["RESTRICTED"],
            contact=["PersonIdentifier4Peppa"],
            theme=["https://mex.rki.de/item/theme-11"],  # INFECTIOUS_DISEASES_AND_...
            title=["some synopse resource title"],
            wasGeneratedBy=["MergedActivityNoORG"],
            unitInCharge=["IdentifierOrgUnitZB"],
            identifier=["SynopseResource"],
        ),
    ]


@pytest.fixture
def mocked_merged_organizational_units() -> list[MergedOrganizationalUnit]:
    """Mock a list of Merged Organizational Unit items."""
    return [
        MergedOrganizationalUnit(
            name=[Text(value="example unit", language="en")],
            parentUnit=MergedOrganizationalUnitIdentifier("identifierParentUnit"),
            email=["unit@example.org"],
            shortName=[Text(value="e.g. unit", language="en")],
            entityType="MergedOrganizationalUnit",
            identifier=MergedOrganizationalUnitIdentifier("IdentifierOrgUnitEG"),
        ),
        MergedOrganizationalUnit(
            name=[Text(value="andere Beispiel unit", language="de")],
            parentUnit=MergedOrganizationalUnitIdentifier("IdentifierOrgUnitEG"),
            email=[],
            shortName=[Text(value="a.bsp. unit", language="en")],
            entityType="MergedOrganizationalUnit",
            identifier=MergedOrganizationalUnitIdentifier("IdentifierOrgUnitZB"),
        ),
        MergedOrganizationalUnit(
            name=[Text(value="non-extractable unit", language="en")],
            parentUnit=MergedOrganizationalUnitIdentifier("identifierParentUnit"),
            email=["dont_extract@example.org"],
            shortName=[Text(value="nope", language="en")],
            entityType="MergedOrganizationalUnit",
            identifier=MergedOrganizationalUnitIdentifier("IdentifierOrgUnitNo"),
        ),
    ]


@pytest.fixture
def mocked_merged_organization() -> list[MergedOrganization]:
    """Mock a list of organizations as Merged Organization items."""
    return [
        MergedOrganization(
            officialName=[Text(value="Organization 2", language="de")],
            entityType="MergedOrganization",
            identifier=MergedOrganizationIdentifier("Identifier2forORG"),
        ),
        MergedOrganization(
            officialName=[Text(value="Organization 1", language=None)],
            entityType="MergedOrganization",
            identifier=MergedOrganizationIdentifier("Identifier1forORG"),
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
            beschreibung="Es handelt sich um ein Projekt/ Vorhaben. Die Nutzung",
            datenhalter="Robert Koch-Institut",
            kontakt=["unit@example.org"],
            organisationseinheit=["a.bsp. unit", "e.g. unit"],
            titel=["short de", "title 'Act' no language", "title en"],
            schlagwort=["Infektionskrankheiten und -epidemiologie"],
            datenbank=[
                "https://www.do-transform.org",
            ],
            voraussetzungen="Unbekannt",
            frequenz="Nicht zutreffend",
            hauptkategorie="Gesundheit",
            unterkategorie="Einflussfaktoren auf die Gesundheit",
            rechtsgrundlage="Nicht bekannt",
            datenerhalt="Externe Zulieferung",
            status="Unbekannt",
            datennutzungszweck="Themenspezifische Auswertung",
            herausgeber="RKI - Robert Koch-Institut",
            kommentar=(
                "Link zum Metadatensatz im RKI Metadatenkatalog wird "
                "voraussichtlich Ende 2025 verfügbar sein."
            ),
            format="Sonstiges",
            identifier="MergedActivityWithORG2",
            entityType="MergedActivity",
        ),
        DatenkompassActivity(
            beschreibung="Es handelt sich um ein Projekt/ Vorhaben. Without language",
            datenhalter="Robert Koch-Institut",
            kontakt=["unit@example.org"],
            organisationseinheit=["e.g. unit"],
            titel=["short only english", "titel de"],
            schlagwort=["Infektionskrankheiten und -epidemiologie"],
            datenbank=[],
            voraussetzungen="Unbekannt",
            frequenz="Nicht zutreffend",
            hauptkategorie="Gesundheit",
            unterkategorie="Einflussfaktoren auf die Gesundheit",
            rechtsgrundlage="Nicht bekannt",
            datenerhalt="Externe Zulieferung",
            status="Unbekannt",
            datennutzungszweck="Themenspezifische Auswertung",
            herausgeber="RKI - Robert Koch-Institut",
            kommentar=(
                "Link zum Metadatensatz im RKI Metadatenkatalog wird "
                "voraussichtlich Ende 2025 verfügbar sein."
            ),
            format="Sonstiges",
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
        "MergedActivity": [mocked_merged_activities[1]],
        "MergedBibliographicResource": mocked_merged_bibliographic_resource,
        "MergedResource": mocked_merged_resource,
        "MergedPrimarySource": mocked_merged_primary_sources,
        "MergedOrganizationalUnit": mocked_merged_organizational_units,
        "MergedOrganization": [mocked_merged_organization[1]],
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
def mocked_boto() -> Generator[MagicMock, None, None]:
    """Mock a S3 session client to write the jsons to."""
    with patch("boto3.Session") as mock_session_class:
        mock_s3_client = MagicMock()
        mock_session_instance = MagicMock()
        mock_session_instance.client.return_value = mock_s3_client
        mock_session_class.return_value = mock_session_instance

        yield mock_s3_client
