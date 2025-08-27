from typing import TypeVar, cast

from bs4 import BeautifulSoup

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
    Theme,
)
from mex.extractors.datenkompass.models.item import (
    DatenkompassActivity,
    DatenkompassBibliographicResource,
    DatenkompassResource,
)
from mex.extractors.settings import Settings

_VocabularyT = TypeVar(
    "_VocabularyT",
    Theme,
    BibliographicResourceType,
    Frequency,
    License,
)


def fix_quotes(string: str) -> str:
    """Fix quote characters in titles or descriptions.

    Removes surrounding (leading and trailing) double quotes and
    replaces in-string double quotes with single quotes.

    Args:
        string: The string to fix quotes for.

    Returns:
        The fixed string.
    """
    return string.strip('"').replace('"', "'")


def get_unit_shortname(
    responsible_unit_ids: list[MergedOrganizationalUnitIdentifier],
    merged_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
) -> list[str]:
    """Get shortName of merged units.

    Args:
        responsible_unit_ids: List of responsible unit identifiers
        merged_units_by_id: dict of all merged organizational units by id

    Returns:
        List of short names of contact units as strings.
    """
    return [
        shortname
        for org_id in responsible_unit_ids
        for shortname in [
            unit_short_name.value
            for unit_short_name in merged_units_by_id[org_id].shortName
        ]
    ]


def get_email(
    responsible_unit_ids: list[MergedOrganizationalUnitIdentifier],
    merged_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
) -> str | None:
    """Get the first email address of referenced responsible units.

    Args:
        responsible_unit_ids: List of responsible unit identifiers
        merged_units_by_id: dict of all merged organizational units by id

    Returns:
        first found email of a responsible unit as string, or None if no email is found.
    """
    return next(
        (
            str(email)
            for org_id in responsible_unit_ids
            for email in merged_units_by_id[org_id].email
        ),
        None,
    )


def get_resource_email(
    responsible_reference_ids: list[
        MergedOrganizationalUnitIdentifier
        | MergedPersonIdentifier
        | MergedContactPointIdentifier
    ],
    merged_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
    merged_contact_points_by_id: dict[MergedContactPointIdentifier, MergedContactPoint],
) -> str | None:
    """Get the first email address of referenced responsible units or contact points.

    Args:
        responsible_reference_ids: Sequence of referenced unit or contact point ids
        merged_units_by_id: dict of all merged organizational units by id
        merged_contact_points_by_id: Dict of all merged contact points by id

    Returns:
        first found email of a unit or contact as string, or None if no email is found.
    """
    combined_dict = cast(
        "dict[Identifier, MergedContactPoint | MergedOrganizationalUnit]",
        {**merged_units_by_id, **merged_contact_points_by_id},
    )

    for reference_id in responsible_reference_ids:
        if (
            referenced_item := combined_dict.get(reference_id)
        ) and referenced_item.email:
            return next(str(email) for email in referenced_item.email)
    return None


def get_title(item: MergedActivity) -> list[str]:
    """Get shortName and title from merged activity item.

    Args:
        item: MergedActivity item.

    Returns:
        List of short name and title of units as strings.
    """
    collected_titles = []
    if item.shortName:
        shortname_de = [
            fix_quotes(name.value) for name in item.shortName if name.language == "de"
        ]
        shortname = (
            shortname_de
            if shortname_de
            else [fix_quotes(name.value) for name in item.shortName]
        )
        collected_titles.extend(shortname)
    if item.title:
        title_de = [
            fix_quotes(name.value) for name in item.title if name.language == "de"
        ]
        title = (
            title_de if title_de else [fix_quotes(name.value) for name in item.title]
        )
        collected_titles.extend(title)
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
    merged_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
) -> list[DatenkompassActivity]:
    """Transform merged to datenkompass activities.

    Args:
        filtered_merged_activities: List of merged activities
        merged_units_by_id: dict of merged organizational units by id

    Returns:
        list of DatenkompassActivity instances.
    """
    settings = Settings()
    delim = settings.datenkompass.list_delimiter
    datenkompass_activities = []
    for item in filtered_merged_activities:
        beschreibung = "Es handelt sich um ein Projekt/ Vorhaben. "
        if item.abstract:
            abstract_de = [
                fix_quotes(a.value) for a in item.abstract if a.language == "de"
            ]
            beschreibung += delim.join(
                abstract_de
                if abstract_de
                else [fix_quotes(a.value) for a in item.abstract]
            )
        kontakt = get_email(
            item.responsibleUnit,
            merged_units_by_id,
        )
        organisationseinheit = get_unit_shortname(
            item.responsibleUnit,
            merged_units_by_id,
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
                identifier=item.identifier,
                entityType=item.entityType,
            ),
        )
    return datenkompass_activities


def transform_bibliographic_resources(
    merged_bibliographic_resources: list[MergedBibliographicResource],
    merged_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
    person_name_by_id: dict[MergedPersonIdentifier, str],
) -> list[DatenkompassBibliographicResource]:
    """Transform merged to datenkompass bibliographic resources.

    Args:
        merged_bibliographic_resources: List of merged bibliographic resources
        merged_units_by_id: dict of merged organizational units by id
        person_name_by_id: dictionary of merged person names by id

    Returns:
        list of DatenkompassBibliographicResource instances.
    """
    settings = Settings()
    delim = settings.datenkompass.list_delimiter
    datenkompass_bibliographic_recources = []
    for item in merged_bibliographic_resources:
        if item.accessRestriction == AccessRestriction["RESTRICTED"]:
            voraussetzungen = "Zugang eingeschränkt"
        elif item.accessRestriction == AccessRestriction["OPEN"]:
            voraussetzungen = "Frei zugänglich"
        else:
            voraussetzungen = None
        datenbank = get_datenbank(item)
        kontakt = get_email(item.contributingUnit, merged_units_by_id)
        organisationseinheit = get_unit_shortname(
            item.contributingUnit, merged_units_by_id
        )
        max_number_authors_cutoff = settings.datenkompass.cutoff_number_authors
        title_collection = ", ".join(fix_quotes(entry.value) for entry in item.title)
        creator_collection = " / ".join(
            [person_name_by_id[c] for c in item.creator[:max_number_authors_cutoff]]
        )
        if len(item.creator) > max_number_authors_cutoff:
            creator_collection += " / et al."
        titel = f"{title_collection} ({creator_collection})"
        vocab = get_vocabulary(item.bibliographicResourceType)
        b1 = f"{delim.join(s for s in vocab if s is not None)}. "
        b2 = delim.join([fix_quotes(abstract.value) for abstract in item.abstract])
        beschreibung = b1 + b2
        datenkompass_bibliographic_recources.append(
            DatenkompassBibliographicResource(
                beschreibung=beschreibung,
                voraussetzungen=voraussetzungen,
                datenbank=datenbank,
                rechtsgrundlagen_benennung=None,
                datennutzungszweck_erweitert=None,
                dk_format="Sonstiges",
                kontakt=kontakt,
                organisationseinheit=organisationseinheit,
                schlagwort=[word.value for word in item.keyword],
                titel=titel,
                datenhalter="Robert Koch-Institut",
                frequenz="Nicht zutreffend",
                hauptkategorie="Gesundheit",
                unterkategorie="Einflussfaktoren auf die Gesundheit",
                datenerhalt="Abruf über eine externe Internetseite oder eine Datenbank",
                status="Stabil",
                datennutzungszweck="Sonstige",
                rechtsgrundlage="Nicht zutreffend",
                herausgeber="RKI - Robert Koch-Institut",
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
    merged_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
    merged_contact_points_by_id: dict[MergedContactPointIdentifier, MergedContactPoint],
) -> list[DatenkompassResource]:
    """Transform merged to datenkompass resources.

    Args:
        merged_resources_by_primary_source: dictionary of merged resources
        merged_units_by_id: dict of merged organizational units by id
        merged_contact_points_by_id: dict of merged contact points

    Returns:
        list of DatenkompassResource instances.
    """
    settings = Settings()
    delim = settings.datenkompass.list_delimiter
    datenkompass_recources = []
    datennutzungszweck_by_primary_source = {
        "report-server": [
            "Themenspezifische Auswertung",
            "Themenspezifisches Monitoring",
        ],
        "open-data": ["Themenspezifische Auswertung"],
        "unit filter": ["Themenspezifisches Monitoring"],
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
            kontakt = get_resource_email(
                item.contact,
                merged_units_by_id,
                merged_contact_points_by_id,
            )
            organisationseinheit = get_unit_shortname(
                item.unitInCharge, merged_units_by_id
            )
            beschreibung = "n/a"
            if item.description:
                description_de = [
                    fix_quotes(d.value) for d in item.description if d.language == "de"
                ]
                beschreibung = delim.join(
                    description_de
                    if description_de
                    else [fix_quotes(d.value) for d in item.description]
                )
                beschreibung_soup = BeautifulSoup(beschreibung, "html.parser")
                for a in beschreibung_soup.find_all("a", href=True):
                    a.replace_with(a["href"])
                beschreibung = str(beschreibung_soup)
            rechtsgrundlagen_benennung = [
                *[entry.value for entry in item.hasLegalBasis],
                *get_vocabulary([item.license] if item.license else []),
            ]
            schlagwort = [
                *get_vocabulary(item.theme),
                *[entry.value for entry in item.keyword],
            ]
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
                    dk_format="Sonstiges",
                    titel=[fix_quotes(t.value) for t in item.title],
                    datenhalter="Robert Koch-Institut",
                    hauptkategorie="Gesundheit",
                    unterkategorie="Einflussfaktoren auf die Gesundheit",
                    rechtsgrundlage="Nicht zutreffend",
                    datenerhalt="Externe Zulieferung",
                    status="Stabil",
                    datennutzungszweck=datennutzungszweck,
                    herausgeber="RKI - Robert Koch-Institut",
                    kommentar=(
                        "Link zum Metadatensatz im RKI Metadatenkatalog wird "
                        "voraussichtlich Ende 2025 verfügbar sein."
                    ),
                    identifier=item.identifier,
                    entityType=item.entityType,
                ),
            )
    return datenkompass_recources
