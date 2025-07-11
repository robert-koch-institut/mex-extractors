import re
from datetime import datetime

from pydantic import TypeAdapter, ValidationError

from mex.common.models import (
    BibliographicResourceMapping,
    ConsentMapping,
    ExtractedBibliographicResource,
    ExtractedConsent,
    ExtractedOrganization,
    ExtractedPerson,
    ExtractedPrimarySource,
)
from mex.common.models.bibliographic_resource import DoiStr
from mex.common.types import (
    UTC,
    AccessRestriction,
    Language,
    MergedOrganizationalUnitIdentifier,
    MergedOrganizationIdentifier,
    TemporalEntity,
    Text,
    TextLanguage,
)
from mex.extractors.endnote.model import EndnoteRecord
from mex.extractors.sinks import load
from mex.extractors.wikidata.helpers import (
    get_wikidata_extracted_organization_id_by_name,
)


def extract_endnote_persons_by_person_string(
    endnote_records: list[EndnoteRecord],
    extracted_primary_source_endnote: ExtractedPrimarySource,
) -> dict[str, ExtractedPerson]:
    """Extract endnote persons.

    Args:
        endnote_records: list of endnote record
        extracted_primary_source_endnote: primary source for endnote

    Returns:
        extracted persons by person string
    """
    unique_persons = {
        author.strip()
        for record in endnote_records
        for author in [
            *record.authors,
            *record.secondary_authors,
            *record.tertiary_authors,
        ]
    }
    extracted_persons: dict[str, ExtractedPerson] = {}
    for person in unique_persons:
        if "," in person:
            if len(split_name := person.split(",")) == 2 and split_name[1]:  # noqa: PLR2004
                family_name, given_name = split_name
                extracted_persons[person] = ExtractedPerson(
                    identifierInPrimarySource=f"Person_{person}",
                    hadPrimarySource=extracted_primary_source_endnote.stableTargetId,
                    familyName=family_name,
                    givenName=given_name,
                    fullName=person,
                )
            else:
                continue
        else:
            extracted_persons[person] = ExtractedPerson(
                identifierInPrimarySource=f"Person_{person}",
                hadPrimarySource=extracted_primary_source_endnote.stableTargetId,
                fullName=person,
            )

    return extracted_persons


def extract_endnote_consents(
    extracted_endnote_persons: list[ExtractedPerson],
    extracted_primary_source_endnote: ExtractedPrimarySource,
    endnote_consent_mapping: ConsentMapping,
) -> list[ExtractedConsent]:
    """Extract endnote consent.

    Args:
        extracted_endnote_persons: list of endnote  persons
        extracted_primary_source_endnote: primary source for endnote
        endnote_consent_mapping: endnote consent mapping default values

    Returns:
        list of extracted consent
    """
    return [
        ExtractedConsent(
            hadPrimarySource=extracted_primary_source_endnote.stableTargetId,
            hasConsentStatus=endnote_consent_mapping.hasConsentStatus[0]
            .mappingRules[0]
            .setValues,
            hasDataSubject=person.stableTargetId,
            identifierInPrimarySource=f"{person.stableTargetId}_consent",
            isIndicatedAtTime=datetime.now(tz=UTC),
        )
        for person in extracted_endnote_persons
    ]


def get_doi(
    electronic_resource_num: str | None,
    endnote_bibliographic_resource: BibliographicResourceMapping,
) -> str | None:
    """Extract doi string and validate.

    Args:
        electronic_resource_num: list of electronic resource num string
        endnote_bibliographic_resource: bibliographic resource mapping

    Returns:
        doi string or None
    """
    doi_adapter = TypeAdapter(DoiStr)
    doi_string = electronic_resource_num
    if not doi_string:
        return None
    if (
        for_value := endnote_bibliographic_resource.doi[0]  # type: ignore[index]
        .mappingRules[1]
        .forValues[0]
    ) and doi_string.startswith(for_value):
        return None
    if doi_string.startswith("10."):
        doi = f"https://doi.org/{doi_string}"
    else:
        doi = doi_string
    try:
        doi_adapter.validate_python(doi)
    except ValidationError:
        return None
    return doi


def extract_endnote_bibliographic_resource(  # noqa: C901, PLR0915
    endnote_records: list[EndnoteRecord],
    endnote_bib_resource_mapping: BibliographicResourceMapping,
    extracted_endnote_persons_by_person_string: dict[str, ExtractedPerson],
    unit_stable_target_ids_by_synonym: dict[str, MergedOrganizationalUnitIdentifier],
    extracted_primary_source_endnote: ExtractedPrimarySource,
) -> list[ExtractedBibliographicResource]:
    """Extract endnote bibliographic resources.

    Args:
        endnote_records: list of endnote record
        endnote_bib_resource_mapping: bibliographical resource mapping
        extracted_endnote_persons_by_person_string: extracted endnote persons by name
        unit_stable_target_ids_by_synonym: Unit stable target ids by synonym
        extracted_primary_source_endnote: primary source for endnote

    Return:
        list of extracted bibliographic resource
    """
    language_by_language_field = {
        for_value: rule.setValues[0]
        for rule in endnote_bib_resource_mapping.language[0].mappingRules
        if rule.forValues and rule.setValues
        for for_value in rule.forValues
    }
    access_restriction_by_custom6: dict[str | None, AccessRestriction | None] = {
        "default": endnote_bib_resource_mapping.accessRestriction[0]
        .mappingRules[2]
        .setValues,
    }
    access_restriction_by_custom6.update(
        {
            for_value: rule.setValues
            for rule in endnote_bib_resource_mapping.accessRestriction[0].mappingRules
            if rule.forValues and rule.setValues
            for for_value in rule.forValues
        }
    )
    ern_for_value = (
        endnote_bib_resource_mapping.alternateIdentifier[0]  # type: ignore[index]
        .mappingRules[0]
        .forValues[0]
    )
    ern_set_value = (
        endnote_bib_resource_mapping.alternateIdentifier[0]  # type: ignore[index]
        .mappingRules[0]
        .setValues[0]
    )
    cn_for_value = (
        endnote_bib_resource_mapping.alternateIdentifier[1]  # type: ignore[index]
        .mappingRules[1]
        .forValues[0]
    )
    cn_set_value = (
        endnote_bib_resource_mapping.alternateIdentifier[1]  # type: ignore[index]
        .mappingRules[1]
        .setValues[0]
    )
    bibliographical_resource_type_by_ref_type = {
        "default": (
            endnote_bib_resource_mapping.bibliographicResourceType[0]
            .mappingRules[2]
            .setValues[0]
        )
        if endnote_bib_resource_mapping.bibliographicResourceType[0]
        .mappingRules[2]
        .setValues
        else None
    }
    bibliographical_resource_type_by_ref_type.update(
        {
            for_value: rule.setValues[0]
            for rule in endnote_bib_resource_mapping.bibliographicResourceType[
                0
            ].mappingRules
            if rule.forValues and rule.setValues
            for for_value in rule.forValues
        }
    )
    bibliographical_resources: list[ExtractedBibliographicResource] = []
    for record in endnote_records:
        language = (
            language_by_language_field.get(record.language, Language["ENGLISH"])
            if record.language
            else Language["ENGLISH"]
        )
        text_language = (
            TextLanguage.DE if language == Language["GERMAN"] else TextLanguage.EN
        )
        abstract = (
            [Text(value=record.abstract, language=text_language)]
            if record.abstract
            else []
        )
        access_restriction = access_restriction_by_custom6.get(
            record.custom6, access_restriction_by_custom6["default"]
        )
        alternate_identifier_ern = (
            ern.replace(ern_for_value, ern_set_value)
            if (ern := record.electronic_resource_num) and ern_for_value in ern
            else None
        )
        alternate_identifier_cn = (
            cn.replace(cn_for_value, cn_set_value)
            if record.call_num and cn_for_value in (cn := record.call_num)
            else None
        )
        alternate_identifier = [
            aid for aid in [alternate_identifier_ern, alternate_identifier_cn] if aid
        ]
        contributing_unit = (
            [
                unit_stable_target_ids_by_synonym[unit.strip()]
                for unit in record.custom4.split(";")
                if unit.strip() in unit_stable_target_ids_by_synonym
            ]
            if record.custom4
            else []
        )
        creator = [
            extracted_endnote_persons_by_person_string[author].stableTargetId
            for author in record.authors
            if author in extracted_endnote_persons_by_person_string
        ]
        if len(creator) == 0:
            continue
        doi = get_doi(record.electronic_resource_num, endnote_bib_resource_mapping)
        editor = [
            extracted_endnote_persons_by_person_string[author].stableTargetId
            for author in record.secondary_authors
        ]
        editor_of_series = [
            extracted_endnote_persons_by_person_string[author].stableTargetId
            for author in record.tertiary_authors
        ]
        issued: TemporalEntity | None = None
        for pub_date in record.pub_dates:
            try:
                issued = TemporalEntity(f"{pub_date} {record.year}")
                break
            except (ValidationError, ValueError):
                continue

        journal = [
            Text(value=f"{record.ref_type} {periodical}", language=text_language)
            for periodical in record.periodical
            if record.ref_type == "Journal Article"
        ]

        keyword = [
            Text(value=keyword, language=text_language) for keyword in record.keyword
        ]
        pages = (
            record.pages
            if record.pages
            and (
                for_value := endnote_bib_resource_mapping.pages[0]  # type: ignore[index]
                .mappingRules[0]
                .forValues[0]
            )
            and re.match(for_value, record.pages)
            else None
        )
        publisher: list[MergedOrganizationIdentifier] = []
        for publisher_string in [record.publisher, record.custom3]:
            if publisher_string is None:
                continue
            if publisher_org := get_wikidata_extracted_organization_id_by_name(
                publisher_string
            ):
                publisher.append(publisher_org)
            else:
                created_org = ExtractedOrganization(
                    hadPrimarySource=extracted_primary_source_endnote.stableTargetId,
                    identifierInPrimarySource=f"Organization_{publisher_string}",
                    officialName=[publisher_string],
                )
                load([created_org])
                publisher.append(created_org.stableTargetId)
        repository_url = record.related_urls[0] if record.related_urls else []
        title_of_series = []
        if record.ref_type == "Book Section":
            if record.periodical:
                title_of_series.extend(
                    [
                        Text(value=title, language=text_language)
                        for title in record.periodical
                    ]
                )
            if record.secondary_title:
                title_of_series.append(
                    Text(value=record.secondary_title, language=text_language),
                )
        bibliographical_resources.append(
            ExtractedBibliographicResource(
                abstract=abstract,
                accessRestriction=access_restriction,
                alternateIdentifier=alternate_identifier,
                bibliographicResourceType=bibliographical_resource_type_by_ref_type.get(
                    record.ref_type,
                    bibliographical_resource_type_by_ref_type["default"],
                ),
                contributingUnit=contributing_unit,
                creator=creator,
                doi=doi,
                editor=editor,
                editorOfSeries=editor_of_series,
                hadPrimarySource=extracted_primary_source_endnote.stableTargetId,
                identifierInPrimarySource=f"{record.database}::{record.rec_number}",
                isbnIssn=[record.isbn] if record.isbn else [],
                issue=record.number,
                issued=issued,
                journal=journal,
                keyword=keyword,
                language=language,
                pages=pages,
                publicationYear=record.year,
                publisher=publisher,
                repositoryURL=repository_url,
                title=Text(value=record.title, language=text_language),
                titleOfSeries=title_of_series,
                volume=record.volume,
            )
        )
    return bibliographical_resources
