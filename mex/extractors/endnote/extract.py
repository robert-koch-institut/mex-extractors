import xml.etree.ElementTree as ET

import defusedxml.ElementTree as defused_ET

from mex.extractors.drop import DropApiConnector
from mex.extractors.endnote.model import EndnoteRecord


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
    connector = DropApiConnector.get()
    file_names = connector.list_files("endnote")
    results: list[EndnoteRecord] = []
    cutoff_number_authors = 50
    for file_name in file_names:
        response = connector.get_raw_file("endnote", file_name)
        file = defused_ET.fromstring(response.content)

        for record in file.findall("*/record"):
            if (
                len(
                    authors := findall_text_from_record(
                        record, "contributors/authors/author/style"
                    )
                )
                >= cutoff_number_authors
            ):
                continue
            results.append(
                EndnoteRecord(
                    database=find_text_from_record(record, "database"),
                    abstract=find_text_from_record(record, "abstract/style"),
                    authors=authors,
                    call_num=find_text_from_record(record, "call-num/style"),
                    custom3=find_text_from_record(record, "custom3/style"),
                    custom4=find_text_from_record(record, "custom4/style"),
                    custom6=find_text_from_record(record, "custom6/style"),
                    electronic_resource_num=find_text_from_record(
                        record, "electronic-resource-num/style"
                    ),
                    isbn=find_text_from_record(record, "isbn/style"),
                    keyword=findall_text_from_record(record, "keywords/keyword/style"),
                    language=find_text_from_record(record, "language/style"),
                    number=find_text_from_record(record, "number/style"),
                    pages=find_text_from_record(record, "pages/style"),
                    periodical=findall_text_from_record(
                        record, "periodical/full-title/style"
                    ),
                    pub_dates=findall_text_from_record(
                        record, "dates/pub-dates/date/style"
                    ),
                    publisher=find_text_from_record(record, "publisher/style"),
                    rec_number=find_text_from_record(record, "rec-number"),
                    ref_type=ref_type.get("name")  # ref_type is stored differently
                    if isinstance(ref_type := record.find("ref-type"), ET.Element)
                    else None,
                    related_urls=findall_text_from_record(
                        record, "urls/related-urls/url/style"
                    ),
                    secondary_authors=findall_text_from_record(
                        record, "contributors/secondary-authors/author/style"
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
