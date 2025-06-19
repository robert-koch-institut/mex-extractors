from mex.common.exceptions import MExError
from mex.common.models import MergedActivity, MergedOrganizationalUnit
from mex.common.types import (
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
)
from mex.common.types.vocabulary import Theme
from mex.extractors.datenkompass.extract import get_merged_items
from mex.extractors.datenkompass.source import DatenkompassActivity


def get_contact(
    responsible_unit_ids: list[MergedOrganizationalUnitIdentifier],
    all_units: list[MergedOrganizationalUnit],
) -> list[str]:
    """Get shortName and email from merged units."""
    return [
        str(s.value) if hasattr(s, "value") else str(s)
        for responsibleUnit in responsible_unit_ids
        for target_unit in all_units
        if target_unit.identifier == responsibleUnit
        for s in target_unit.shortName + target_unit.email
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


def get_vocabulary(themes: list[Theme]) -> list[str | None]:
    """Get german prefLabel for Theme."""
    return [
        next(
            concept.prefLabel.de
            for concept in Theme.__concepts__
            if str(concept.identifier)
            == Theme[str(theme_entry).removeprefix("Theme.")].value
        )
        for theme_entry in themes
    ]


def check_halter(
    bmg_ids: set[MergedOrganizationIdentifier],
    halter: list[MergedOrganizationIdentifier],
) -> str:
    """Check if 'Datenhalter' is really the BMG."""
    if any(halter_id in bmg_ids for halter_id in halter):
        return "BMG"
    msg = "'Datenhalter' is not BMG!"
    raise MExError(msg)


def transform_to_target_fields(
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
        if item.abstract:
            abstract_de = [a.value for a in item.abstract if a.language == "de"]
            beschreibung = abstract_de[0] if abstract_de else item.abstract[0].value
        else:
            beschreibung = None
        kontakt = get_contact(item.responsibleUnit, all_units)
        titel = get_title(item)
        schlagwort = get_vocabulary(item.theme)
        halter = check_halter(bmg_ids, item.funderOrCommissioner)
        datenkompass_activities.append(
            DatenkompassActivity(
                Beschreibung=beschreibung,
                Halter=halter,
                Kontakt=kontakt,
                Titel=titel,
                Schlagwort=schlagwort,
                Datenbank=[str(entry) for entry in item.website],
                Voraussetzungen="Unbekannt",
                Hauptkategorie="Gesundheit",
                Unterkategorie="Public Health",
                Rechtsgrundlage="Nicht bekannt",
                Weg="Externe Zulieferung",
                Status="Unbekannt",
                Datennutzungszweck="Themenspezifische Auswertung",
                Herausgeber="Robert Koch-Institut",
                Kommentar=(
                    "Link zum Metadatensatz im RKI Metadatenkatalog wird "
                    "voraussichtlich Ende 2025 verfügbar sein.)"
                ),
                Format="Projekt/Vorhaben",
                identifier=item.identifier,
                entityType=item.entityType,
            )
        )
    return datenkompass_activities
