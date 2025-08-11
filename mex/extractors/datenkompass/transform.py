from typing import TypeVar, cast

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
    MergedContactPointIdentifier,
    MergedOrganizationalUnitIdentifier,
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

_VocabularyT = TypeVar(
    "_VocabularyT",
    Theme,
    BibliographicResourceType,
    Frequency,
    License,
    ResourceCreationMethod,
    ResourceTypeGeneral,
)


def get_unit_shortname(
    responsible_unit_ids: list[MergedOrganizationalUnitIdentifier],
    merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
) -> list[str]:
    """Get shortName of merged units.

    Args:
        responsible_unit_ids: List of responsible unit identifiers
        merged_organizational_units_by_id: dict of all merged organizational units by id

    Returns:
        List of short names of contact units as strings.
    """
    return [
        shortname
        for org_id in responsible_unit_ids
        for shortname in [
            unit_short_name.value
            for unit_short_name in merged_organizational_units_by_id[org_id].shortName
        ]
    ]


def get_email(
    responsible_unit_ids: list[MergedOrganizationalUnitIdentifier],
    merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
) -> list[str]:
    """Get email of merged units.

    Args:
        responsible_unit_ids: List of responsible unit identifiers
        merged_organizational_units_by_id: dict of all merged organizational units by id

    Returns:
        List of emails of contact units as strings.
    """
    return [
        email
        for org_id in responsible_unit_ids
        for email in [
            str(unit_email)
            for unit_email in merged_organizational_units_by_id[org_id].email
        ]
    ]


def get_resource_contact(
    responsible_unit_ids: list[
        MergedOrganizationalUnitIdentifier
        | MergedPersonIdentifier
        | MergedContactPointIdentifier
    ],
    merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
    merged_contact_points_by_id: dict[MergedContactPointIdentifier, MergedContactPoint],
) -> list[str]:
    """Get email from units and contact points.

    Args:
        responsible_unit_ids: Set of responsible unit identifiers
        merged_organizational_units_by_id: dict of all merged organizational units by id
        merged_contact_points_by_id: Dict of all merged contact points by id

    Returns:
        List of email-addresses as strings.
    """
    contact_details: list[str] = []
    combined_dict = cast(
        "dict[Identifier, MergedContactPoint | MergedOrganizationalUnit]",
        {**merged_organizational_units_by_id, **merged_contact_points_by_id},
    )

    for contact_id in responsible_unit_ids:
        if contact := combined_dict.get(contact_id):
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
        collected_titles.append(shortname.strip('"').replace('"', "'"))
    if item.title:
        title_de = [name.value for name in item.title if name.language == "de"]
        title = title_de[0] if title_de else item.title[0].value
        collected_titles.append(title.strip('"').replace('"', "'"))
    return collected_titles


def get_vocabulary(
    entries: list[_VocabularyT],
) -> list[str | None]:
    """Get german prefLabel for Vocabularies.

    Args:
        entries: list of vocabulary type entries.

    Returns:
        list of german Vocabulary entries as strings.
    """
    return [
        next(
            concept.prefLabel.de
            for concept in type(entry).__concepts__
            if str(concept.identifier) == entry.value
        )
        for entry in entries
    ]


def get_datenbank(item: MergedBibliographicResource) -> str | None:
    """Get first doi url or first repository URL.

    Args:
        item: MergedBibliographicResource item.

    Returns:
        url as string.
    """
    if item.doi:
        return item.doi
    if item.repositoryURL:
        return item.repositoryURL[0].url
    return None


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
        beschreibung = "Es handelt sich um ein Projekt/ Vorhaben."
        if item.abstract:
            beschreibung += " "
            abstract_de = [a.value for a in item.abstract if a.language == "de"]
            beschreibung += abstract_de[0] if abstract_de else item.abstract[0].value
        beschreibung = beschreibung.strip('"').replace('"', "'")
        kontakt = get_email(
            item.responsibleUnit,
            merged_organizational_units_by_id,
        )
        organisationseinheit = get_unit_shortname(
            item.responsibleUnit,
            merged_organizational_units_by_id,
        )
        titel = get_title(item)
        schlagwort = get_vocabulary(item.theme)
        datenbank = []
        if item.website:
            url_de = [w.url for w in item.website if w.language == "de"]
            datenbank += url_de if url_de else [item.website[0].url]
        datenkompass_activities.append(
            DatenkompassActivity(
                datenhalter="Robert Koch-Institut",
                beschreibung=beschreibung,
                kontakt=kontakt,
                organisationseinheit=organisationseinheit,
                titel=titel,
                schlagwort=schlagwort,
                datenbank=datenbank,
                frequenz="Nicht zutreffend",
                hauptkategorie="Gesundheit",
                unterkategorie="Einflussfaktoren auf die Gesundheit",
                rechtsgrundlage="Nicht bekannt",
                datenerhalt="Externe Zulieferung",
                status="Unbekannt",
                datennutzungszweck="Themenspezifische Auswertung",
                herausgeber="Robert Koch-Institut",
                kommentar=(
                    "Link zum Metadatensatz im RKI Metadatenkatalog wird "
                    "voraussichtlich Ende 2025 verfügbar sein."
                ),
                format="Sonstiges",
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
    person_name_by_id: dict[MergedPersonIdentifier, str],
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
        kontakt = get_email(item.contributingUnit, merged_organizational_units_by_id)
        organisationseinheit = get_unit_shortname(
            item.contributingUnit, merged_organizational_units_by_id
        )
        max_number_authors_cutoff = 5
        title_collection = ", ".join(
            entry.value.strip('"').replace('"', "'") for entry in item.title
        )
        creator_collection = " / ".join(
            [person_name_by_id[c] for c in item.creator[:max_number_authors_cutoff]]
        )
        if len(item.creator) > max_number_authors_cutoff:
            creator_collection += " / et al."
        titel = f"{title_collection} ({creator_collection})"
        datenkompass_bibliographic_recources.append(
            DatenkompassBibliographicResource(
                beschreibung=[*get_vocabulary(item.bibliographicResourceType), "."]
                + [
                    abstract.value.strip('"').replace('"', "'")
                    for abstract in item.abstract
                ],
                voraussetzungen=voraussetzungen,
                datenbank=datenbank,
                dk_format="Sonstiges",
                kontakt=kontakt,
                organisationseinheit=organisationseinheit,
                schlagwort=[word.value for word in item.keyword],
                titel=titel,
                datenhalter="Robert Koch-Institut",
                frequenz="Einmalig",
                hauptkategorie="Gesundheit",
                unterkategorie="Einflussfaktoren auf die Gesundheit",
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
    merged_resources_by_primary_source: dict[str, list[MergedResource]],
    merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
    merged_contact_points_by_id: dict[MergedContactPointIdentifier, MergedContactPoint],
) -> list[DatenkompassResource]:
    """Transform merged to datenkompass resources.

    Args:
        merged_resources_by_primary_source: dictionary of merged resources
        merged_organizational_units_by_id: dict of merged organizational units by id
        merged_contact_points_by_id: dict of merged contact points

    Returns:
        list of DatenkompassResource instances.
    """
    datenkompass_recources = []
    datennutzungszweck_by_primary_source = {
        "report-server": [
            "Themenspezifische Auswertung",
            "Themenspezifisches Monitoring",
        ],
        "open-data": ["Themenspezifische Auswertung"],
    }
    for (
        primary_source,
        merged_resources_list,
    ) in merged_resources_by_primary_source.items():
        for item in merged_resources_list:
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
                item.contact,
                merged_organizational_units_by_id,
                merged_contact_points_by_id,
            )
            organisationseinheit = get_unit_shortname(
                item.unitInCharge, merged_organizational_units_by_id
            )
            beschreibung = "n/a"
            if item.description:
                description_de = [
                    d.value.strip('"').replace('"', "'")
                    for d in item.description
                    if d.language == "de"
                ]
                beschreibung = (
                    (description_de[0] if description_de else item.description[0].value)
                    .strip('"')
                    .replace('"', "'")
                )
            rechtsgrundlagen_benennung = [
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
            rechtsgrundlage = (
                "Ja" if (item.hasLegalBasis or item.license) else "Nicht bekannt"
            )
            datennutzungszweck = datennutzungszweck_by_primary_source[primary_source]
            datenkompass_recources.append(
                DatenkompassResource(
                    voraussetzungen=voraussetzungen,
                    frequenz=frequenz,
                    kontakt=kontakt,
                    organisationseinheit=organisationseinheit,
                    beschreibung=beschreibung,
                    datenbank=item.doi,
                    rechtsgrundlagen_benennung=rechtsgrundlagen_benennung,
                    datennutzungszweck_erweitert=[hp.value for hp in item.hasPurpose],
                    schlagwort=schlagwort,
                    dk_format=dk_format,
                    titel=[t.value.strip('"').replace('"', "'") for t in item.title],
                    datenhalter="Robert Koch-Institut",
                    hauptkategorie="Gesundheit",
                    unterkategorie="Einflussfaktoren auf die Gesundheit",
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
                    primary_source=primary_source,
                ),
            )
    return datenkompass_recources
