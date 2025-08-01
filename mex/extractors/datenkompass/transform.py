from collections.abc import Iterable
from typing import cast

from mex.common.models import (
    MergedActivity,
    MergedBibliographicResource,
    MergedContactPoint,
    MergedOrganizationalUnit,
    MergedResource,
)
from mex.common.types import (
    AccessRestriction,
    Identifier,
    Link,
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    MergedPersonIdentifier,
)
from mex.common.types.vocabulary import (
    BibliographicResourceType,
    Frequency,
    License,
    ResourceCreationMethod,
    ResourceTypeGeneral,
    Theme,
)
from mex.extractors.datenkompass.models.item import (
    DatenkompassActivity,
    DatenkompassBibliographicResource,
    DatenkompassResource,
)


def get_contact(
    responsible_unit_ids: list[MergedOrganizationalUnitIdentifier],
    merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
) -> list[str]:
    """Get shortName and email from merged units.

    Args:
        responsible_unit_ids: List of responsible unit identifiers
        merged_organizational_units_by_id: dict of all merged organizational units by id

    Returns:
        List of short name and email of contact units as strings.
    """
    return [
        contact
        for org_id in responsible_unit_ids
        for contact in [
            short_name.value
            for short_name in merged_organizational_units_by_id[org_id].shortName
        ]
        + [str(email) for email in merged_organizational_units_by_id[org_id].email]
    ]


def get_resource_contact(
    responsible_unit_ids: list[Identifier],
    merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
    merged_contact_points_by_id: dict[MergedContactPointIdentifier, MergedContactPoint],
) -> list[str]:
    """Get email from units and contact points and shortname from units.

    Args:
        responsible_unit_ids: Set of responsible unit identifiers
        merged_organizational_units_by_id: dict of all merged organizational units by id
        merged_contact_points_by_id: Dict of all merged contact points by id

    Returns:
        List of shortnames and email-addresses as strings.
    """
    contact_details: list[str] = []
    combined_dict = cast(
        "dict[Identifier, MergedContactPoint | MergedOrganizationalUnit]",
        {**merged_organizational_units_by_id, **merged_contact_points_by_id},
    )

    for contact_id in responsible_unit_ids:
        if contact := combined_dict.get(contact_id):
            if contact.entityType == "MergedOrganizationalUnit":
                contact_details.extend(
                    [short_name.value for short_name in contact.shortName]
                )
            contact_details.extend([str(email) for email in contact.email])

    return contact_details


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
    entries: Iterable[
        Theme
        | BibliographicResourceType
        | Frequency
        | License
        | ResourceCreationMethod
        | ResourceTypeGeneral
    ],  # "list doesn't work well with '|' "
) -> list[str | None]:
    """Get german prefLabel for Vocabularies.

    Args:
        entries: Iterable of vocabulary entries.

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
    filtered_merged_activities: list[MergedActivity],
    merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
) -> list[DatenkompassActivity]:
    """Transform merged to datenkompass activities.

    Args:
        filtered_merged_activities: List of merged activities
        merged_organizational_units_by_id: dict of merged organizational units by id

    Returns:
        list of DatenkompassActivity instances.
    """
    datenkompass_activities = []
    for item in filtered_merged_activities:
        beschreibung = None
        if item.abstract:
            abstract_de = [a.value for a in item.abstract if a.language == "de"]
            beschreibung = abstract_de[0] if abstract_de else item.abstract[0].value
        kontakt = get_contact(
            item.responsibleUnit,
            merged_organizational_units_by_id,
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
            ),
        )
    return datenkompass_activities


def transform_bibliographic_resources(
    merged_bibliographic_resources: list[MergedBibliographicResource],
    merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
    person_name_by_id: dict[MergedPersonIdentifier, list[str]],
) -> list[DatenkompassBibliographicResource]:
    """Transform merged to datenkompass bibliographic resources.

    Args:
        merged_bibliographic_resources: List of merged bibliographic resources
        merged_organizational_units_by_id: dict of merged organizational units by id
        person_name_by_id: dictionary of merged person names by id

    Returns:
        list of DatenkompassBibliographicResource instances.
    """
    datenkompass_bibliographic_recources = []
    for item in merged_bibliographic_resources:
        if item.accessRestriction == AccessRestriction["RESTRICTED"]:
            voraussetzungen = "Zugang eingeschränkt"
        elif item.accessRestriction == AccessRestriction["OPEN"]:
            voraussetzungen = "Frei zugänglich"
        else:
            voraussetzungen = None
        datenbank = get_datenbank(item)
        dk_format = get_vocabulary(item.bibliographicResourceType)
        kontakt = get_contact(item.contributingUnit, merged_organizational_units_by_id)
        title_list = ", ".join(entry.value for entry in item.title)
        creator_list = " / ".join(
            [" / ".join(person_name_by_id[c]) for c in item.creator]
        )
        titel = f"{title_list} ({creator_list})"
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
            ),
        )
    return datenkompass_bibliographic_recources


def transform_resources(
    merged_resources: dict[str, list[MergedResource]],
    filtered_merged_activities: list[MergedActivity],
    merged_bmg_ids: set[MergedOrganizationIdentifier],
    merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
    merged_contact_points_by_id: dict[MergedContactPointIdentifier, MergedContactPoint],
) -> list[DatenkompassResource]:
    """Get the relevant info from the merged resources.

    Args:
        merged_resources: List of merged resources
        filtered_merged_activities: list of merged activities
        merged_bmg_ids: set of merged bmg organization identifiers
        merged_organizational_units_by_id: dict of merged organizational units by id
        merged_contact_points_by_id: dict of merged contact points

    Returns:
        list of DatenkompassResource instances.
    """
    datenkompass_recources = []
    merged_activities_set = {
        ma.identifier
        for ma in filtered_merged_activities
        if any(fOC in merged_bmg_ids for fOC in ma.funderOrCommissioner)
    }
    for primary_source, list_merged_resources in merged_resources.items():
        for item in list_merged_resources:
            if item.accessRestriction == AccessRestriction["RESTRICTED"]:
                voraussetzungen = "Zugang eingeschränkt"
            elif item.accessRestriction == AccessRestriction["OPEN"]:
                voraussetzungen = "Frei zugänglich"
            frequenz = (
                get_vocabulary([item.accrualPeriodicity])
                if item.accrualPeriodicity
                else []
            )
            kontakt = get_resource_contact(
                sorted({*item.contact, *item.unitInCharge}),
                merged_organizational_units_by_id,
                merged_contact_points_by_id,
            )
            beschreibung = "n/a"
            if item.description:
                description_de = [
                    d.value for d in item.description if d.language == "de"
                ]
                beschreibung = (
                    description_de[0] if description_de else item.description[0].value
                )
            rechtsgrundlagenbenennung = [
                *[entry.value for entry in item.hasLegalBasis],
                *get_vocabulary([item.license] if item.license else []),
            ]
            schlagwort = [
                *get_vocabulary(item.theme),
                *[entry.value for entry in item.keyword],
            ]
            dk_format = [
                *get_vocabulary(item.resourceCreationMethod),
                *get_vocabulary(item.resourceTypeGeneral),
            ]
            unterkategorie = ["Public Health"]
            if primary_source == "Synopse":
                unterkategorie += ["Gesundheitliche Lage"]
            datenhalter = (
                "BMG" if item.wasGeneratedBy in merged_activities_set else None
            )
            rechtsgrundlage = (
                "Ja" if (item.hasLegalBasis or item.license) else "Nicht bekannt"
            )
            datennutzungszweck = ["Themenspezifische Auswertung"]
            if primary_source == "Synopse":
                datennutzungszweck += ["Themenspezifisches Monitoring"]
            datenkompass_recources.append(
                DatenkompassResource(
                    voraussetzungen=voraussetzungen,
                    frequenz=frequenz,
                    kontakt=kontakt,
                    beschreibung=beschreibung,
                    datenbank=item.doi,
                    rechtsgrundlagenbenennung=rechtsgrundlagenbenennung,
                    datennutzungszweckerweitert=[hp.value for hp in item.hasPurpose],
                    schlagwort=schlagwort,
                    dk_format=dk_format,
                    titel=[t.value for t in item.title],
                    datenhalter=datenhalter,
                    hauptkategorie="Gesundheit",
                    unterkategorie=unterkategorie,
                    rechtsgrundlage=rechtsgrundlage,
                    datenerhalt="Externe Zulieferung",
                    status="Stabil",
                    datennutzungszweck=datennutzungszweck,
                    herausgeber="Robert Koch-Institut",
                    kommentar=(
                        "Link zum Metadatensatz im RKI Metadatenkatalog wird "
                        "voraussichtlich Ende 2025 verfügbar sein."
                    ),
                    identifier=item.identifier,
                    entityType=item.entityType,
                ),
            )
    return datenkompass_recources
