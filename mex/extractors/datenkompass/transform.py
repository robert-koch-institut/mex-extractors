from typing import TYPE_CHECKING, TypeVar, cast

from bs4 import BeautifulSoup

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
from mex.extractors.utils import load_yaml

if TYPE_CHECKING:
    from pydantic import BaseModel

    from mex.common.models import (
        MergedActivity,
        MergedBibliographicResource,
        MergedContactPoint,
        MergedOrganizationalUnit,
        MergedResource,
    )
    from mex.common.types import (
        Identifier,
        MergedContactPointIdentifier,
        MergedOrganizationalUnitIdentifier,
        MergedPersonIdentifier,
        Text,
    )
    from mex.extractors.datenkompass.models.mapping import DatenkompassMappingField

VocabularyT = TypeVar(
    "VocabularyT",
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
) -> str | None:
    """Get shortName of merged units.

    Args:
        responsible_unit_ids: List of responsible unit identifiers
        merged_organizational_units_by_id: dict of all merged organizational units by id
        delim: delimiter for joining short name entries

    Returns:
        List of short names of contact units as strings.
    """
    if responsible_unit_ids and (
        result := delim.join(
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
    ):
        return result
    return None


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


def get_german_vocabulary[
    VocabularyT: (Theme, BibliographicResourceType, Frequency, License)
](
    entries: list[VocabularyT] | None,
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


def mapping_lookup_default(
    model: type[BaseModel],
    mapping: DatenkompassMapping,
) -> dict[str, DatenkompassMappingField]:
    """Create a dictionary of fields by field name of Datenkompass mappings.

    For this the alias name needs to be used as intermediate step, because the alias
    (not the field name) is the identifier in the mapping.

    Args:
        model: Datenkompass model.
        mapping: Datenkompass mapping.

    Returns:
        dictionary of mapping field names to values.
    """
    return {
        field_name: field
        for field_name in model.model_fields
        for field in mapping.fields
        if field.fieldInTarget
        and field.fieldInTarget == model.model_fields[field_name].alias
    }


def handle_setval(set_value: list[str] | str | None) -> str:
    """Return value of mapping setValues as string, even if setValues is a list.

    Args:
        set_value: setValues value of mapping

    Returns:
        stringified value of setValues.
    """
    settings = Settings.get()
    delim = settings.datenkompass.list_delimiter

    if isinstance(set_value, str):
        return set_value
    if isinstance(set_value, list):
        return delim.join(set_value)
    msg = "no default value set in mapping."
    raise ValueError(msg)


def built_string_shorter_than_limit(
    words: list[str | None],
    delim: str,
    min_word_length: int,
    max_string_length: int,
) -> str:
    """Filter out certain words and limit final string to maximum length.

    Args:
        words: list of entries.
        delim: list delimiter for joining the strings in list
        min_word_length: minimal length of each word.
        max_string_length: maximal length of final string of joined words.

    Returns:
        combined string.
    """
    parts = []
    total_length = 0

    for word in words:
        if not word or any(
            url_pattern in word for url_pattern in ["www.", "http:", "https:"]
        ):
            continue

        word_len = len(word)

        if word_len < min_word_length:
            continue

        if total_length + word_len > max_string_length:
            break

        parts.append(word)
        total_length += word_len + len(delim)

    return delim.join(parts)


def transform_activities(
    filtered_merged_activities: list[MergedActivity],
    merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier,
        MergedOrganizationalUnit,
    ],
    activity_mapping: DatenkompassMapping,
) -> list[DatenkompassActivity]:
    """Transform merged to datenkompass activities.

    Args:
        filtered_merged_activities: List of merged activities
        merged_organizational_units_by_id: dict of merged organizational units by id
        activity_mapping: Datenkompass mapping.

    Returns:
        list of DatenkompassActivity instances.
    """
    settings = Settings.get()
    delim = settings.datenkompass.list_delimiter
    datenkompass_activities = []
    default_by_fieldname = mapping_lookup_default(
        DatenkompassActivity,
        activity_mapping,
    )
    for item in filtered_merged_activities:
        beschreibung = handle_setval(
            default_by_fieldname["beschreibung"].mappingRules[0].setValues
        )
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
            datenbank = url_de or item.website[0].url
        datenhalter = handle_setval(
            default_by_fieldname["datenhalter"].mappingRules[0].setValues
        )
        start = str(item.start[0]) if item.start else None
        end = str(item.end[0]) if item.end else None
        zeitliche_abdeckung = f"{start} - {end}" if start and end else start or end
        voraussetzungen = handle_setval(
            default_by_fieldname["voraussetzungen"].mappingRules[0].setValues
        )
        frequenz = handle_setval(
            default_by_fieldname["frequenz"].mappingRules[0].setValues
        )
        hauptkategorie = handle_setval(
            default_by_fieldname["hauptkategorie"].mappingRules[0].setValues
        )
        unterkategorie = handle_setval(
            default_by_fieldname["unterkategorie"].mappingRules[0].setValues
        )
        rechtsgrundlage = handle_setval(
            default_by_fieldname["rechtsgrundlage"].mappingRules[0].setValues
        )
        datenerhalt = handle_setval(
            default_by_fieldname["datenerhalt"].mappingRules[0].setValues
        )
        status = handle_setval(default_by_fieldname["status"].mappingRules[0].setValues)
        datennutzungszweck = handle_setval(
            default_by_fieldname["datennutzungszweck"].mappingRules[0].setValues
        )
        herausgeber = handle_setval(
            default_by_fieldname["herausgeber"].mappingRules[0].setValues
        )
        kommentar = handle_setval(
            default_by_fieldname["kommentar"].mappingRules[0].setValues
        )
        dk_format = handle_setval(
            default_by_fieldname["dk_format"].mappingRules[0].setValues
        )
        datenkompass_activities.append(
            DatenkompassActivity(
                datenhalter=datenhalter,
                beschreibung=beschreibung,
                kontakt=kontakt,
                organisationseinheit=organisationseinheit,
                titel=titel,
                schlagwort=schlagwort,
                datenbank=datenbank,
                zeitliche_abdeckung=zeitliche_abdeckung,
                voraussetzungen=voraussetzungen,
                frequenz=frequenz,
                hauptkategorie=hauptkategorie,
                unterkategorie=unterkategorie,
                rechtsgrundlage=rechtsgrundlage,
                datenerhalt=datenerhalt,
                status=status,
                datennutzungszweck=datennutzungszweck,
                herausgeber=herausgeber,
                kommentar=kommentar,
                dk_format=dk_format,
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
    datenkompass_person_str_by_id: dict[MergedPersonIdentifier, str],
    bibliographic_resource_mapping: DatenkompassMapping,
) -> list[DatenkompassBibliographicResource]:
    """Transform merged to datenkompass bibliographic resources.

    Args:
        merged_bibliographic_resources: List of merged bibliographic resources
        merged_organizational_units_by_id: dict of merged organizational units by id
        datenkompass_person_str_by_id: dictionary of merged person names by id
        bibliographic_resource_mapping: Datenkompass mapping.

    Returns:
        list of DatenkompassBibliographicResource instances.
    """
    settings = Settings.get()
    delim = settings.datenkompass.list_delimiter
    datenkompass_bibliographic_recources = []
    default_by_fieldname = mapping_lookup_default(
        DatenkompassBibliographicResource,
        bibliographic_resource_mapping,
    )
    for item in merged_bibliographic_resources:
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
            [
                datenkompass_person_str_by_id[c]
                for c in item.creator[:max_number_authors_cutoff]
            ]
        )
        if len(item.creator) > max_number_authors_cutoff:
            creator_collection += " / et al."
        titel = f"{title_collection} ({creator_collection})"
        vocab = get_german_vocabulary(item.bibliographicResourceType)
        beschreibung = f"{delim.join(v for v in vocab if v is not None)}. "
        beschreibung += get_abstract_or_description(item.abstract, delim)
        voraussetzungen = next(
            handle_setval(rule.setValues)
            for rule in default_by_fieldname["voraussetzungen"].mappingRules
            if rule.forValues and rule.forValues[0] == item.accessRestriction.name
        )
        rechtsgrundlagen_benennung = handle_setval(
            default_by_fieldname["rechtsgrundlagen_benennung"].mappingRules[0].setValues
        )
        datennutzungszweck_erweitert = handle_setval(
            default_by_fieldname["datennutzungszweck_erweitert"]
            .mappingRules[0]
            .setValues
        )
        dk_format = handle_setval(
            default_by_fieldname["dk_format"].mappingRules[0].setValues
        )
        datenhalter = handle_setval(
            default_by_fieldname["datenhalter"].mappingRules[0].setValues
        )
        frequenz = handle_setval(
            default_by_fieldname["frequenz"].mappingRules[0].setValues
        )
        hauptkategorie = handle_setval(
            default_by_fieldname["hauptkategorie"].mappingRules[0].setValues
        )
        unterkategorie = handle_setval(
            default_by_fieldname["unterkategorie"].mappingRules[0].setValues
        )
        herausgeber = handle_setval(
            default_by_fieldname["herausgeber"].mappingRules[0].setValues
        )
        datenerhalt = handle_setval(
            default_by_fieldname["datenerhalt"].mappingRules[0].setValues
        )
        status = handle_setval(default_by_fieldname["status"].mappingRules[0].setValues)
        datennutzungszweck = handle_setval(
            default_by_fieldname["datennutzungszweck"].mappingRules[0].setValues
        )
        rechtsgrundlage = handle_setval(
            default_by_fieldname["rechtsgrundlage"].mappingRules[0].setValues
        )
        kommentar = handle_setval(
            default_by_fieldname["kommentar"].mappingRules[0].setValues
        )
        schlagwort = (
            delim.join([word.value for word in item.keyword]) if item.keyword else None
        )
        datenkompass_bibliographic_recources.append(
            DatenkompassBibliographicResource(
                beschreibung=beschreibung,
                voraussetzungen=voraussetzungen,
                datenbank=datenbank,
                rechtsgrundlagen_benennung=rechtsgrundlagen_benennung,
                datennutzungszweck_erweitert=datennutzungszweck_erweitert,
                dk_format=dk_format,
                kontakt=kontakt,
                organisationseinheit=organisationseinheit,
                schlagwort=schlagwort,
                titel=titel,
                datenhalter=datenhalter,
                frequenz=frequenz,
                hauptkategorie=hauptkategorie,
                unterkategorie=unterkategorie,
                herausgeber=herausgeber,
                datenerhalt=datenerhalt,
                status=status,
                datennutzungszweck=datennutzungszweck,
                rechtsgrundlage=rechtsgrundlage,
                kommentar=kommentar,
                identifier=item.identifier,
                entityType=item.entityType,
            ),
        )
    return datenkompass_bibliographic_recources


def transform_resources(
    merged_resources_by_primary_source_by_unit: dict[
        str, dict[str, list[MergedResource]]
    ],
    merged_organizational_units_by_id: dict[
        MergedOrganizationalUnitIdentifier,
        MergedOrganizationalUnit,
    ],
    merged_contact_points_by_id: dict[MergedContactPointIdentifier, MergedContactPoint],
) -> dict[str, dict[str, list[DatenkompassResource]]]:
    """Transform merged to datenkompass resources.

    Args:
        merged_resources_by_primary_source_by_unit: dictionary of merged resources
        merged_organizational_units_by_id: dict of merged organizational units by id
        merged_contact_points_by_id: dict of merged contact points

    Returns:
        list of DatenkompassResource instances.
    """
    settings = Settings.get()
    resource_mapping = DatenkompassMapping.model_validate(
        load_yaml(settings.datenkompass.mapping_path / "resource.yaml")
    )
    delim = settings.datenkompass.list_delimiter
    min_keyword_length = settings.datenkompass.min_keyword_item_length
    max_keyword_string_length = settings.datenkompass.max_keyword_str_length

    default_by_fieldname = mapping_lookup_default(
        DatenkompassResource,
        resource_mapping,
    )

    datennutzungszweck = handle_setval(
        default_by_fieldname["datennutzungszweck"].mappingRules[0].setValues
    )
    dk_format = handle_setval(
        default_by_fieldname["dk_format"].mappingRules[0].setValues
    )
    datenhalter = handle_setval(
        default_by_fieldname["datenhalter"].mappingRules[0].setValues
    )
    hauptkategorie = handle_setval(
        default_by_fieldname["hauptkategorie"].mappingRules[0].setValues
    )
    unterkategorie = handle_setval(
        default_by_fieldname["unterkategorie"].mappingRules[0].setValues
    )
    rechtsgrundlage = handle_setval(
        default_by_fieldname["rechtsgrundlage"].mappingRules[0].setValues
    )
    datenerhalt = handle_setval(
        default_by_fieldname["datenerhalt"].mappingRules[0].setValues
    )
    status = handle_setval(default_by_fieldname["status"].mappingRules[0].setValues)
    herausgeber = handle_setval(
        default_by_fieldname["herausgeber"].mappingRules[0].setValues
    )
    kommentar = handle_setval(
        default_by_fieldname["kommentar"].mappingRules[0].setValues
    )

    result_resoures_by_primary_source_by_unit: dict[
        str, dict[str, list[DatenkompassResource]]
    ] = {}
    for unit, inner_dict in merged_resources_by_primary_source_by_unit.items():
        result_resoures_by_primary_source: dict[str, list[DatenkompassResource]] = {}
        for primary_source, merged_resources_list in inner_dict.items():
            datenkompass_resources = []
            for item in merged_resources_list:
                voraussetzungen = next(
                    handle_setval(rule.setValues)
                    for rule in default_by_fieldname["voraussetzungen"].mappingRules
                    if rule.forValues
                    and rule.forValues[0] == item.accessRestriction.name
                )
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
                    else handle_setval(
                        default_by_fieldname["beschreibung"].mappingRules[0].setValues
                    )
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
                schlagwort = built_string_shorter_than_limit(
                    schlagwort_collection,
                    delim,
                    min_keyword_length,
                    max_keyword_string_length,
                )
                datennutzungszweck_erweitert = (
                    delim.join(
                        [hp.value for hp in item.hasPurpose if hp.value is not None]
                    )
                    or None
                )
                datenkompass_resources.append(
                    DatenkompassResource(
                        voraussetzungen=voraussetzungen,
                        frequenz=frequenz,
                        kontakt=kontakt,
                        organisationseinheit=organisationseinheit,
                        startdatum=str(item.created) if item.created else None,
                        beschreibung=beschreibung,
                        datenbank=item.doi,
                        rechtsgrundlagen_benennung=rechtsgrundlagen_benennung,
                        datennutzungszweck_erweitert=datennutzungszweck_erweitert,
                        schlagwort=schlagwort,
                        dk_format=dk_format,
                        titel=delim.join([fix_quotes(t.value) for t in item.title]),
                        datenhalter=datenhalter,
                        hauptkategorie=hauptkategorie,
                        unterkategorie=unterkategorie,
                        rechtsgrundlage=rechtsgrundlage,
                        datenerhalt=datenerhalt,
                        status=status,
                        datennutzungszweck=datennutzungszweck,
                        herausgeber=herausgeber,
                        kommentar=kommentar,
                        identifier=item.identifier,
                        entityType=item.entityType,
                    ),
                )
            result_resoures_by_primary_source[primary_source] = datenkompass_resources
        result_resoures_by_primary_source_by_unit[unit] = (
            result_resoures_by_primary_source
        )
    return result_resoures_by_primary_source_by_unit
