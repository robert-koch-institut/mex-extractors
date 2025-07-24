from collections.abc import Iterable

from mex.common.models import (
    MergedActivity,
    MergedBibliographicResource,
    MergedOrganizationalUnit,
)
from mex.common.types import (
    AccessRestriction,
    Link,
    MergedOrganizationalUnitIdentifier,
    MergedPersonIdentifier,
)
from mex.common.types.vocabulary import BibliographicResourceType, Theme
from mex.extractors.datenkompass.models.item import (
    DatenkompassActivity,
    DatenkompassBibliographicResource,
)


def get_contact(
    responsible_unit_ids: list[MergedOrganizationalUnitIdentifier],
    merged_organizational_units: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
) -> list[str]:
    """Get shortName and email from merged units.

    Args:
        responsible_unit_ids: List of responsible unit identifiers.
        merged_organizational_units: dict of all merged organizational units by id.

    Returns:
        List of short name and email of contact units as strings.
    """
    return [
        contact
        for org_id in responsible_unit_ids
        for unit in [merged_organizational_units[org_id]]
        for contact in [short_name.value for short_name in unit.shortName]
        + [str(email) for email in unit.email]
    ]


def get_title(item: MergedActivity) -> list[str]:
    """Get shortName and title from merged activity item.

    Args:
        item: MergedActivity item.

    Returns:
        List of short name and title of units as strings.
    """
    collected_titles = []
    if item.shortName:
        shortname_de = [name.value for name in item.shortName if name.language == "de"]
        shortname = shortname_de[0] if shortname_de else item.shortName[0].value
        collected_titles.append(shortname)
    if item.title:
        title_de = [name.value for name in item.title if name.language == "de"]
        title = title_de[0] if title_de else item.title[0].value
        collected_titles.append(title)
    return collected_titles


def get_vocabulary(
    entries: Iterable[Theme | BibliographicResourceType],  # "list doesn't accept '|' "
) -> list[str | None]:
    """Get german prefLabel for Vocabularies.

    Args:
        entries: Iterable of Theme or BibliographicResourceType entries.

    Returns:
        list of german Vocabulary entries.
    """
    return [
        next(
            concept.prefLabel.de
            for concept in type(entry).__concepts__
            if str(concept.identifier) == entry.value
        )
        for entry in entries
    ]


def get_datenbank(item: MergedBibliographicResource) -> str:
    """Get Datenbank entries.

    Args:
        item: MergedBibliographicResource item.

    Returns:
        string of concatenated entries.
    """
    return ", ".join(
        entry.url if isinstance(entry, Link) else str(entry)
        for entry in [item.doi, *item.alternateIdentifier, *item.repositoryURL]
    )


def transform_activities(
    extracted_and_filtered_merged_activities: list[MergedActivity],
    extracted_merged_organizational_units: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
) -> list[DatenkompassActivity]:
    """Transform merged to datenkompass activities.

    Args:
        extracted_and_filtered_merged_activities: List of merged activities
        extracted_merged_organizational_units: dict of merged organizational units by id

    Returns:
        list of DatenkompassActivity instances.
    """
    datenkompass_activities = []
    for item in extracted_and_filtered_merged_activities:
        beschreibung = None
        if item.abstract:
            abstract_de = [a.value for a in item.abstract if a.language == "de"]
            beschreibung = abstract_de[0] if abstract_de else item.abstract[0].value
        kontakt = get_contact(
            item.responsibleUnit,
            extracted_merged_organizational_units,
        )
        titel = get_title(item)
        schlagwort = get_vocabulary(item.theme)
        datenkompass_activities.append(
            DatenkompassActivity(
                datenhalter="BMG",
                beschreibung=beschreibung,
                kontakt=kontakt,
                titel=titel,
                schlagwort=schlagwort,
                datenbank=[entry.url for entry in item.website],
                voraussetzungen="Unbekannt",
                hauptkategorie="Gesundheit",
                unterkategorie="Public Health",
                rechtsgrundlage="Nicht bekannt",
                datenerhalt="Externe Zulieferung",
                status="Unbekannt",
                datennutzungszweck="Themenspezifische Auswertung",
                herausgeber="Robert Koch-Institut",
                kommentar=(
                    "Link zum Metadatensatz im RKI Metadatenkatalog wird "
                    "voraussichtlich Ende 2025 verfügbar sein."
                ),
                format="Projekt/Vorhaben",
                identifier=item.identifier,
                entityType=item.entityType,
            )
        )
    return datenkompass_activities


def transform_bibliographic_resources(
    extracted_merged_bibliographic_resources: list[MergedBibliographicResource],
    extracted_merged_organizational_units: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
    person_name_by_id: dict[MergedPersonIdentifier, list[str]],
) -> list[DatenkompassBibliographicResource]:
    """Transform merged to datenkompass bibliographic resources.

    Args:
        extracted_merged_bibliographic_resources: List of merged bibliographic resources
        extracted_merged_organizational_units: dict of merged organizational units by id
        person_name_by_id: dictionary of merged person names by id

    Returns:
        list of DatenkompassBibliographicResource instances.
    """
    datenkompass_bibliographic_recources = []
    for item in extracted_merged_bibliographic_resources:
        if item.accessRestriction == AccessRestriction["RESTRICTED"]:
            voraussetzungen = "Zugang eingeschränkt"
        elif item.accessRestriction == AccessRestriction["OPEN"]:
            voraussetzungen = "Frei zugänglich"
        else:
            voraussetzungen = None
        datenbank = get_datenbank(item)
        dk_format = get_vocabulary(item.bibliographicResourceType)
        kontakt = get_contact(
            item.contributingUnit, extracted_merged_organizational_units
        )
        titel = (
            ", ".join(entry.value for entry in item.title)
            + " ("
            + " / ".join([" / ".join(person_name_by_id[c]) for c in item.creator])
            + ")"
        )
        datenkompass_bibliographic_recources.append(
            DatenkompassBibliographicResource(
                beschreibung=[abstract.value for abstract in item.abstract],
                voraussetzungen=voraussetzungen,
                datenbank=datenbank,
                dk_format=dk_format,
                kontakt=kontakt,
                schlagwort=[word.value for word in item.keyword],
                titel=titel,
                hauptkategorie="Gesundheit",
                unterkategorie="Public Health",
                herausgeber="Robert Koch-Institut",
                kommentar=(
                    "Link zum Metadatensatz im RKI Metadatenkatalog wird "
                    "voraussichtlich Ende 2025 verfügbar sein."
                ),
                identifier=item.identifier,
                entityType=item.entityType,
            )
        )
    return datenkompass_bibliographic_recources
