from collections.abc import Iterable

from mex.common.exceptions import MExError
from mex.common.models import (
    MergedActivity,
    MergedOrganizationalUnit,
)
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
)
from mex.common.types.vocabulary import BibliographicResourceType, Theme
from mex.extractors.datenkompass.extract import get_merged_items
from mex.extractors.datenkompass.item import DatenkompassActivity


def get_contact(
    responsible_unit_ids: list[MergedOrganizationalUnitIdentifier],
    merged_organizational_units: list[MergedOrganizationalUnit],
) -> list[str]:
    """Get shortName and email from merged units."""
    return [
        contact
        for target_unit in all_units
        if target_unit.identifier in responsible_unit_ids
        for contact in [short_name.value for short_name in target_unit.shortName] + [str(email) for email in target_unit.email]
    ]


def get_title(item: MergedActivity) -> list[str]:
    """Get shortName and title from merged activity item."""
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
    entries: Iterable[Theme | BibliographicResourceType],
) -> list[str | None]:
    """Get german prefLabel for Vocabularies."""
    return [
        next(
            concept.prefLabel.de
            for concept in type(entry).__concepts__
            if str(concept.identifier) == entry.value
        )
        for entry in entries
    ]


def check_datenhalter(
    bmg_ids: set[MergedOrganizationIdentifier],
    datenhalter: list[MergedOrganizationIdentifier],
) -> str:
    """Check if 'Datenhalter' is really the BMG."""
    if any(datenhalter_id in bmg_ids for datenhalter_id in datenhalter):
        return "BMG"
    msg = "Funder or Commissioner is not BMG!"
    raise MExError(msg)


def transform_activities(
    extracted_and_filtered_merged_activities: list[MergedActivity],
    all_units: list[MergedOrganizationalUnit],
) -> list[DatenkompassActivity]:
    """Get the info asked for."""
    datenkompass_activities = []
    bmg_ids = {
        MergedOrganizationIdentifier(bmg.identifier)
        for bmg in get_merged_items("BMG", ["MergedOrganization"], None)
    }
    for item in extracted_and_filtered_merged_activities:
        beschreibung = None
        if item.abstract:
            abstract_de = [a.value for a in item.abstract if a.language == "de"]
            beschreibung = abstract_de[0] if abstract_de else item.abstract[0].value      
        kontakt = get_contact(item.responsibleUnit, all_units)
        titel = get_title(item)
        schlagwort = get_vocabulary(item.theme)
        datenhalter = check_datenhalter(bmg_ids, item.funderOrCommissioner)
        datenkompass_activities.append(
            DatenkompassActivity(
                datenhalter=datenhalter,
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
