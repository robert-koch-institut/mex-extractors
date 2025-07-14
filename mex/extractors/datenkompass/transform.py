from collections.abc import Iterable

from mex.common.models import (
    MergedActivity,
    MergedOrganizationalUnit,
)
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
)
from mex.common.types.vocabulary import BibliographicResourceType, Theme
from mex.extractors.datenkompass.models.item import DatenkompassActivity


def get_contact(
    responsible_unit_ids: list[MergedOrganizationalUnitIdentifier],
    merged_organizational_units: list[MergedOrganizationalUnit],
) -> list[str]:
    """Get shortName and email from merged units.

    Args:
        responsible_unit_ids: List of responsible unit identifiers.
        merged_organizational_units: List of merged organizational unit identifiers.

    Returns:
        List of short name and email of contact units as strings.
    """
    return [
        contact
        for target_unit in merged_organizational_units
        if target_unit.identifier in responsible_unit_ids
        for contact in [short_name.value for short_name in target_unit.shortName]
        + [str(email) for email in target_unit.email]
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


def transform_activities(
    extracted_and_filtered_merged_activities: list[MergedActivity],
    merged_organizational_units: list[MergedOrganizationalUnit],
) -> list[DatenkompassActivity]:
    """Get the relevant info from the merged activities.

    Args:
        extracted_and_filtered_merged_activities: List of merged activities.
        merged_organizational_units: List of merged organizational units.

    Returns:
        list of DatenkompassActivity instances.
    """
    datenkompass_activities = []
    for item in extracted_and_filtered_merged_activities:
        beschreibung = None
        if item.abstract:
            abstract_de = [a.value for a in item.abstract if a.language == "de"]
            beschreibung = abstract_de[0] if abstract_de else item.abstract[0].value
        kontakt = get_contact(item.responsibleUnit, merged_organizational_units)
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
                    "voraussichtlich Ende 2025 verfügbar sein.)"
                ),
                format="Projekt/Vorhaben",
                identifier=item.identifier,
                entityType=item.entityType,
            )
        )
    return datenkompass_activities
