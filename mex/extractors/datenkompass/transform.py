from typing import TypeVar, cast

from bs4 import BeautifulSoup
from pydantic import BaseModel

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
    Text,
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
from mex.extractors.datenkompass.models.mapping import DatenkompassMapping
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
    merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
    delim: str,
) -> str:
    """Get shortName of merged units.

    Args:
        responsible_unit_ids: List of responsible unit identifiers
        merged_organizational_units_by_id: dict of all merged organizational units by id
        delim: delimiter for joining short name entries

    Returns:
        List of short names of contact units as strings.
    """
    return delim.join(
        [
            shortname
            for org_id in responsible_unit_ids
            for shortname in [
                unit_short_name.value
                for unit_short_name in merged_organizational_units_by_id[
                    org_id
                ].shortName
            ]
        ]
    )


def get_email(
    responsible_unit_ids: list[MergedOrganizationalUnitIdentifier],
    merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
) -> str | None:
    """Get the first email address of referenced responsible units.

    Args:
        responsible_unit_ids: List of responsible unit identifiers
        merged_organizational_units_by_id: dict of all merged organizational units by id

    Returns:
        first found email of a responsible unit as string, or None if no email is found.
    """
    return next(
        (
            str(email)
            for org_id in responsible_unit_ids
            if org_id in merged_organizational_units_by_id
            for email in merged_organizational_units_by_id[org_id].email
        ),
        None,
    )


def get_resource_email(
    responsible_reference_ids: list[
        MergedOrganizationalUnitIdentifier
        | MergedPersonIdentifier
        | MergedContactPointIdentifier
    ],
    merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier, MergedOrganizationalUnit
    ],
    merged_contact_points_by_id: dict[MergedContactPointIdentifier, MergedContactPoint],
) -> str | None:
    """Get the first email address of referenced responsible units or contact points.

    Ignore referenced Persons.

    Args:
        responsible_reference_ids: List of referenced unit, contact point or person ids
        merged_organizational_units_by_id: dict of all merged organizational units by id
        merged_contact_points_by_id: Dict of all merged contact points by id

    Returns:
        first found email of a unit or contact as string, or None if no email is found.
    """
    combined_dict = cast(
        "dict[Identifier, MergedContactPoint | MergedOrganizationalUnit]",
        {**merged_organizational_units_by_id, **merged_contact_points_by_id},
    )

    for reference_id in responsible_reference_ids:
        if (
            referenced_item := combined_dict.get(reference_id)
        ) and referenced_item.email:
            return next(str(email) for email in referenced_item.email)
    return None


def get_german_text(text_entries: list[Text]) -> list[str]:
    """Get german entries of list as strings, if any exist.

     If no german entry exists, return original list entries as strings.
     Always fix quotes in entries.

    Args:
        text_entries: list of text entries

    Returns:
        list of entries as strings
    """
    return [fix_quotes(t.value) for t in text_entries if t.language == "de"] or [
        fix_quotes(t.value) for t in text_entries
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
        collected_titles.extend(get_german_text(item.shortName))
    if item.title:
        collected_titles.extend(get_german_text(item.title))
    return collected_titles


def get_german_vocabulary(
    entries: list[_VocabularyT] | None,
) -> list[str | None]:
    """Get german prefLabel for Vocabularies.

    Args:
        entries: list of vocabulary type entries.

    Returns:
        list of german Vocabulary entries as strings.
    """
    if entries:
        return [
            next(
                (
                    concept.prefLabel.de
                    for concept in type(entry).__concepts__
                    if str(concept.identifier) == entry.value
                ),
                None,
            )
            for entry in entries
        ]
    return []


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


def get_abstract_or_description(abstracts: list[Text], delim: str) -> str:
    """Get German list entries, join them and reformat html-formated links.

    Args:
        abstracts: list of mixed language strings with possible html-formated links
        delim: list delimiter for joining the strings in list

    Returns:
        joined german strings with reformated plain text urls.
    """
    if not abstracts:
        return ""
    abstract_string = delim.join(get_german_text(abstracts))
    soup_string = BeautifulSoup(abstract_string, "html.parser")
    for a in soup_string.find_all("a", href=True):
        a.replace_with(a["href"])
    return str(soup_string)


def mapping_lookup(
    model: type[BaseModel],
    mapping: DatenkompassMapping,
    delim: str,
    field_names: list[str],
) -> dict[str, str]:
    """Lookup default values in Datenkompass mappings."""
    default_by_fieldname: dict[str, str] = {}
    for field_name in field_names:
        field_info = model.model_fields[field_name]
        alias_name = getattr(field_info, "alias", None)
        for mapping_field in mapping.fields:
            if alias_name == mapping_field.fieldInTarget:
                set_value = mapping_field.mappingRules[0].setValues
                if isinstance(set_value, list):
                    default_by_fieldname[field_name] = delim.join(set_value)
                elif isinstance(set_value, str):
                    default_by_fieldname[field_name] = set_value
                else:
                    raise ValueError()
    return default_by_fieldname

def transform_activities(
    filtered_merged_activities: list[MergedActivity],
    merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier,
        MergedOrganizationalUnit,
    ],
    activity_mapping: DatenkompassMapping
) -> list[DatenkompassActivity]:
    """Transform merged to datenkompass activities.

    Args:
        filtered_merged_activities: List of merged activities
        merged_organizational_units_by_id: dict of merged organizational units by id

    Returns:
        list of DatenkompassActivity instances.
    """
    settings = Settings.get()
    delim = settings.datenkompass.list_delimiter
    datenkompass_activities = []
    default_by_fieldname = mapping_lookup(
        DatenkompassActivity,
        activity_mapping,
        delim,
        [
            "datenhalter",
            "beschreibung",
            "voraussetzungen",
            "frequenz",
            "hauptkategorie",
            "unterkategorie",
            "rechtsgrundlage",
            "datenerhalt",
            "status",
            "datennutzungszweck",
            "herausgeber",
            "kommentar",
            "format",
        ]
    )
    for item in filtered_merged_activities:
        beschreibung = default_by_fieldname["beschreibung"]
        beschreibung += get_abstract_or_description(item.abstract, delim)
        kontakt = get_email(
            item.responsibleUnit,
            merged_organizational_units_by_id,
        )
        organisationseinheit = get_unit_shortname(
            item.responsibleUnit,
            merged_organizational_units_by_id,
            delim,
        )
        titel = delim.join(get_title(item))
        schlagwort = (
            delim.join(t for t in get_german_vocabulary(item.theme) if t is not None)
            or None
        )
        datenbank = None
        if item.website:
            url_de = delim.join([w.url for w in item.website if w.language == "de"])
            datenbank = url_de if url_de else item.website[0].url
        datenkompass_activities.append(
            DatenkompassActivity(
                datenhalter=default_by_fieldname["datenhalter"],
                beschreibung=beschreibung,
                kontakt=kontakt,
                organisationseinheit=organisationseinheit,
                titel=titel,
                schlagwort=schlagwort,
                datenbank=datenbank,
                voraussetzungen=default_by_fieldname["voraussetzungen"],
                frequenz=default_by_fieldname["frequenz"],
                hauptkategorie=default_by_fieldname["hauptkategorie"],
                unterkategorie=default_by_fieldname["unterkategorie"],
                rechtsgrundlage=default_by_fieldname["rechtsgrundlage"],
                datenerhalt=default_by_fieldname["datenerhalt"],
                status=default_by_fieldname["status"],
                datennutzungszweck=default_by_fieldname["datennutzungszweck"],
                herausgeber=default_by_fieldname["herausgeber"],
                kommentar = default_by_fieldname["kommentar"],
                format=default_by_fieldname["format"],
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
    bibliographic_resource_mapping: DatenkompassMapping,
) -> list[DatenkompassBibliographicResource]:
    """Transform merged to datenkompass bibliographic resources.

    Args:
        merged_bibliographic_resources: List of merged bibliographic resources
        merged_organizational_units_by_id: dict of merged organizational units by id
        person_name_by_id: dictionary of merged person names by id

    Returns:
        list of DatenkompassBibliographicResource instances.
    """
    settings = Settings.get()
    delim = settings.datenkompass.list_delimiter
    datenkompass_bibliographic_recources = []
    default_by_fieldname = mapping_lookup(
        DatenkompassBibliographicResource,
        bibliographic_resource_mapping,
        delim,
        [
            "rechtsgrundlagen_benennung",
            "datennutzungszweck_erweitert",
            "dk_format",
            "datenhalter",
            "frequenz",
            "hauptkategorie",
            "unterkategorie",
            "herausgeber",
            "datenerhalt",
            "status",
            "datennutzungszweck",
            "rechtsgrundlage",
            "kommentar",
        ]
    )
    for item in merged_bibliographic_resources:
        if item.accessRestriction == AccessRestriction["RESTRICTED"]:
            voraussetzungen = "Zugang eingeschränkt"
        elif item.accessRestriction == AccessRestriction["OPEN"]:
            voraussetzungen = "Frei zugänglich"
        datenbank = get_datenbank(item)
        kontakt = get_email(item.contributingUnit, merged_organizational_units_by_id)
        organisationseinheit = get_unit_shortname(
            item.contributingUnit,
            merged_organizational_units_by_id,
            delim,
        )
        max_number_authors_cutoff = settings.datenkompass.cutoff_number_authors
        title_collection = ", ".join(fix_quotes(entry.value) for entry in item.title)
        creator_collection = " / ".join(
            [person_name_by_id[c] for c in item.creator[:max_number_authors_cutoff]]
        )
        if len(item.creator) > max_number_authors_cutoff:
            creator_collection += " / et al."
        titel = f"{title_collection} ({creator_collection})"
        vocab = get_german_vocabulary(item.bibliographicResourceType)
        beschreibung = f"{delim.join(v for v in vocab if v is not None)}. "
        beschreibung += get_abstract_or_description(item.abstract, delim)
        datenkompass_bibliographic_recources.append(
            DatenkompassBibliographicResource(
                beschreibung=beschreibung,
                voraussetzungen=voraussetzungen,
                datenbank=datenbank,
                rechtsgrundlagen_benennung=default_by_fieldname["rechtsgrundlagen_benennung"],
                datennutzungszweck_erweitert=default_by_fieldname["datennutzungszweck_erweitert"],
                dk_format=default_by_fieldname["dk_format"],
                kontakt=kontakt,
                organisationseinheit=organisationseinheit,
                schlagwort=delim.join([word.value for word in item.keyword]),
                titel=titel,
                datenhalter=default_by_fieldname["datenhalter"],
                frequenz=default_by_fieldname["frequenz"],
                hauptkategorie=default_by_fieldname["hauptkategorie"],
                unterkategorie=default_by_fieldname["unterkategorie"],
                herausgeber=default_by_fieldname["herausgeber"],
                datenerhalt=default_by_fieldname["datenerhalt"],
                status=default_by_fieldname["status"],
                datennutzungszweck=default_by_fieldname["datennutzungszweck"],
                rechtsgrundlage=default_by_fieldname["rechtsgrundlage"],
                kommentar=default_by_fieldname["kommentar"],
                identifier=item.identifier,
                entityType=item.entityType,
            ),
        )
    return datenkompass_bibliographic_recources


def transform_resources(
    merged_resources_by_primary_source: dict[str, list[MergedResource]],
    merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier,
        MergedOrganizationalUnit,
    ],
    merged_contact_points_by_id: dict[MergedContactPointIdentifier, MergedContactPoint],
    resource_mapping: DatenkompassMapping,
) -> list[DatenkompassResource]:
    """Transform merged to datenkompass resources.

    Args:
        merged_resources_by_primary_source: dictionary of merged resources
        merged_organizational_units_by_id: dict of merged organizational units by id
        merged_contact_points_by_id: dict of merged contact points

    Returns:
        list of DatenkompassResource instances.
    """
    settings = Settings.get()
    delim = settings.datenkompass.list_delimiter
    datenkompass_recources = []
    default_by_fieldname = mapping_lookup(
        DatenkompassResource,
        resource_mapping,
        delim,
        [
            "dk_format",
            "datenhalter",
            "hauptkategorie",
            "unterkategorie",
            "rechtsgrundlage",
            "datenerhalt",
            "status",
            "herausgeber",
            "kommentar",
        ]
    )
    datennutzungszweck_by_primary_source = {
        "report-server": "Themenspezifische Auswertung; Themenspezifisches Monitoring",
        "open-data": "Themenspezifische Auswertung",
        "unit filter": "Themenspezifisches Monitoring",
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
            frequenz_vocabulary = (
                get_german_vocabulary([item.accrualPeriodicity])
                if item.accrualPeriodicity
                else []
            )
            frequenz = (
                delim.join(f for f in frequenz_vocabulary if f is not None) or None
            )
            kontakt = get_resource_email(
                item.contact,
                merged_organizational_units_by_id,
                merged_contact_points_by_id,
            )
            organisationseinheit = get_unit_shortname(
                item.unitInCharge,
                merged_organizational_units_by_id,
                delim,
            )
            beschreibung = (
                get_abstract_or_description(item.description, delim)
                if item.description
                else "n/a"
            )
            rechtsgrundlagen_benennung_collection = [
                entry.value for entry in item.hasLegalBasis
            ] + get_german_vocabulary(
                [item.license] if item.license else [],
            )
            rechtsgrundlagen_benennung = (
                delim.join(
                    entry
                    for entry in rechtsgrundlagen_benennung_collection
                    if entry is not None
                )
                or None
            )
            schlagwort_collection = get_german_vocabulary(item.theme) + [
                entry.value for entry in item.keyword
            ]
            schlagwort = (
                delim.join(
                    [entry for entry in schlagwort_collection if entry is not None]
                )
                or None
            )
            datennutzungszweck_erweitert = (
                delim.join([hp.value for hp in item.hasPurpose if hp.value is not None])
                or None
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
                    datennutzungszweck_erweitert=datennutzungszweck_erweitert,
                    schlagwort=schlagwort,
                    dk_format=default_by_fieldname["dk_format"],
                    titel=delim.join([fix_quotes(t.value) for t in item.title]),
                    datenhalter=default_by_fieldname["datenhalter"],
                    hauptkategorie=default_by_fieldname["hauptkategorie"],
                    unterkategorie=default_by_fieldname["unterkategorie"],
                    rechtsgrundlage=default_by_fieldname["rechtsgrundlage"],
                    datenerhalt=default_by_fieldname["datenerhalt"],
                    status=default_by_fieldname["status"],
                    datennutzungszweck=datennutzungszweck,
                    herausgeber=default_by_fieldname["herausgeber"],
                    kommentar=default_by_fieldname["kommentar"],
                    identifier=item.identifier,
                    entityType=item.entityType,
                ),
            )
    return datenkompass_recources
