import xml.etree.ElementTree as ET
from pathlib import Path

import defusedxml.ElementTree as defused_ET

from mex.extractors.endnote.model import EndnoteRecord
from mex.extractors.settings import Settings


def extract_endnote_records() -> list[EndnoteRecord]:
    """Extract endnote records by loading data from XML files.

    Returns:
        list of endnote records
    """
    settings = Settings.get()
    files: list[ET.ElementTree] = []
    for file in Path(settings.endnote.raw_data_path).glob("*.xml"):
        with file.open(encoding="utf-8") as fh:
            files.append(defused_ET.parse(fh))
    return [
        EndnoteRecord.model_validate(
            {
                "database": database.text
                if isinstance(database := record.find("database"), ET.Element)
                else None,
                "abstract": abstract.text
                if isinstance(abstract := record.find("abstract/style"), ET.Element)
                else None,
                "authors": [
                    author.text
                    for author in record.findall("contributors/authors/author/style")
                ],
                "call_num": call_num.text
                if isinstance(call_num := record.find("call-num/style"), ET.Element)
                else None,
                "custom4": custom4.text
                if isinstance(custom4 := record.find("custom4/style"), ET.Element)
                else None,
                "custom6": custom6.text
                if isinstance(custom6 := record.find("custom6/style"), ET.Element)
                else None,
                "edition": edition.text
                if isinstance(edition := record.find("edition/style"), ET.Element)
                else None,
                "electronic_resource_num": ern.text
                if isinstance(
                    ern := record.find("electronic-resource-num/style"), ET.Element
                )
                else None,
                "isbn": isbn.text
                if isinstance(isbn := record.find("isbn/style"), ET.Element)
                else None,
                "keyword": [
                    keyword.text for keyword in record.findall("keywords/keyword/style")
                ],
                "language": language.text
                if isinstance(language := record.find("language/style"), ET.Element)
                else None,
                "number": number.text
                if isinstance(number := record.find("number/style"), ET.Element)
                else None,
                "pages": pages.text
                if isinstance(pages := record.find("pages/style"), ET.Element)
                else None,
                "periodical": [
                    periodical.text
                    for periodical in record.findall("periodical/full-title/style")
                ],
                "pub_dates": [
                    pub_date.text
                    for pub_date in record.findall("dates/pub-dates/date/style")
                ],
                "publisher": publisher.text
                if isinstance(publisher := record.find("publisher/style"), ET.Element)
                else None,
                "rec_number": rec_number.text
                if isinstance(rec_number := record.find("rec-number"), ET.Element)
                else None,
                "ref_type": ref_type.get("name")
                if isinstance(ref_type := record.find("ref-type"), ET.Element)
                else None,
                "related_urls": [
                    related_url.text
                    for related_url in record.findall("urls/related-urls/url/style")
                ],
                "secondary_authors": [
                    secondary_author.text
                    for secondary_author in record.findall(
                        "contributors/secondary-authors/author/style"
                    )
                ],
                "secondary_title": secondary_title.text
                if isinstance(
                    secondary_title := record.find("titles/secondary-title/style"),
                    ET.Element,
                )
                else None,
                "tertiary_authors": [
                    tertiary_author.text
                    for tertiary_author in record.findall(
                        "contributors/tertiary-authors/author/style"
                    )
                ],
                "title": title.text
                if isinstance(title := record.find("titles/title/style"), ET.Element)
                else None,
                "volume": volume.text
                if isinstance(volume := record.find("volume/style"), ET.Element)
                else None,
                "year": year.text
                if isinstance(year := record.find("dates/year/style"), ET.Element)
                else None,
            }
        )
        for file in files
        for record in file.getroot().findall("*/record")
    ]
