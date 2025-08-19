import xml.etree.ElementTree as ET

import defusedxml.ElementTree as defused_ET

from mex.extractors.drop import DropApiConnector
from mex.extractors.endnote.model import EndnoteRecord
from mex.extractors.settings import Settings


def findall_text_from_record(record: ET.Element, path: str) -> list[str]:
    """Finds all instances of items in path."""
    return ["".join(node.itertext()) for node in record.findall(path)]


def find_text_from_record(record: ET.Element, path: str) -> str | None:
    """Finds the instance of an item in path."""
    if isinstance(node := record.find(path), ET.Element):
        return "".join(node.itertext())
    return None


def extract_endnote_records() -> list[EndnoteRecord]:
    """Extract endnote records by loading XML files from MEx drop.

    Returns:
        list of endnote records
    """
    settings = Settings.get()
    connector = DropApiConnector.get()
    file_names = connector.list_files("endnote")
    results: list[EndnoteRecord] = []
    for file_name in file_names:
        response = connector.get_raw_file("endnote", file_name)
        file = defused_ET.fromstring(response.content)

        for record in file.findall("*/record"):
            if (
                len(
                    authors := findall_text_from_record(
                        record, "contributors/authors/author"
                    )
                )
                >= settings.endnote.cutoff_number_authors
            ):
                continue
            results.append(
                EndnoteRecord(
                    database=find_text_from_record(record, "database"),
                    abstract=find_text_from_record(record, "abstract"),
                    authors=authors,
                    call_num=find_text_from_record(record, "call-num"),
                    custom3=find_text_from_record(record, "custom3"),
                    custom4=find_text_from_record(record, "custom4"),
                    custom6=find_text_from_record(record, "custom6"),
                    electronic_resource_num=find_text_from_record(
                        record, "electronic-resource-num"
                    ),
                    isbn=find_text_from_record(record, "isbn"),
                    keyword=findall_text_from_record(record, "keywords/keyword"),
                    language=find_text_from_record(record, "language"),
                    number=find_text_from_record(record, "number"),
                    pages=find_text_from_record(record, "pages"),
                    periodical=findall_text_from_record(
                        record, "periodical/full-title"
                    ),
                    pub_dates=findall_text_from_record(record, "dates/pub-dates/date"),
                    publisher=find_text_from_record(record, "publisher"),
                    rec_number=find_text_from_record(record, "rec-number"),
                    ref_type=ref_type.get("name")  # ref_type is stored differently
                    if isinstance(ref_type := record.find("ref-type"), ET.Element)
                    else None,
                    related_urls=findall_text_from_record(
                        record, "urls/related-urls/url"
                    ),
                    secondary_authors=findall_text_from_record(
                        record, "contributors/secondary-authors/author"
                    ),
                    secondary_title=find_text_from_record(
                        record, "titles/secondary-title/style"
                    ),
                    tertiary_authors=findall_text_from_record(
                        record, "contributors/tertiary-authors/author/style"
                    ),
                    title=find_text_from_record(record, "titles/title/style"),
                    volume=find_text_from_record(record, "volume/style"),
                    year=find_text_from_record(record, "dates/year/style"),
                )
            )
    return results
