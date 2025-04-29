import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

from mex.common.exceptions import MExError
from mex.extractors.datscha_web.models.item import DatschaWebItem


def parse_item_urls_from_overview_html(html_data: str, url: str) -> list[str]:
    """Parse the item url from the overview page.

    Args:
        html_data: Raw HTML data
        url: Datscha URL prefix

    Returns:
        List of parsed URLs
    """
    soup = BeautifulSoup(html_data, "html.parser")
    urls = []
    for row in soup.find_all("tr", attrs={"class": ["tr_1", "tr_2"]}):
        rel_link = row.a.get("href")
        urls.append(urljoin(url, rel_link))
    return urls


def parse_single_item_html(html_data: str, item_url: str) -> DatschaWebItem:
    """Parse a single Datscha item from a details page.

    Args:
        html_data: Raw HTML
        item_url: Datscha item url

    Raises:
        MExError: When the URL does not contain an ID

    Returns:
        Parsed datscha item
    """
    details: dict[str, int | str | list[str]] = {}

    item_id_match = re.search(r".*\?.*vavs_id=(\d+).*", item_url)
    if item_id_match is None:
        msg = f"Malformed item url missing vavs_id: {item_url}"
        raise MExError(msg)
    details["item_id"] = int(item_id_match.groups()[0])

    soup = BeautifulSoup(html_data, "html.parser")
    content_box = soup.select("div .standard_form")[0]

    for detail_block in content_box.select("div .detail_block"):
        field_name, field_value = parse_detail_block(detail_block)
        details[field_name] = field_value

    unit_key, unit_value = parse_unit_loz(soup)
    details[unit_key] = unit_value

    return DatschaWebItem.model_validate(details)


def parse_detail_block(detail_block: Tag) -> tuple[str, str]:
    """Get values of first divs with classes "input_vorgabe" and "input_feld".

    Args:
        detail_block: BeautifulSoup tag element

    Returns:
        First values for "input_vorgabe" and "input_feld"
    """
    key = detail_block.find("div", {"class": "input_vorgabe"})
    value = detail_block.find("div", {"class": "input_feld"})

    if not isinstance(key, Tag):
        msg = (
            f"Missing div tag of class 'input_vorgabe' in detail_block:\n"
            f"{detail_block.prettify()}"
        )
        raise MExError(msg)
    if not isinstance(value, Tag):
        msg = (
            f"Missing div tag of class 'input_feld' in detail_block.\n"
            f"{detail_block.prettify()}"
        )
        raise MExError(msg)

    key_clean = str(key.string).rstrip(":")
    value_clean = " ".join(str(value.string).split())
    return key_clean, value_clean


def parse_unit_loz(bs4_object: BeautifulSoup) -> tuple[str, list[str]]:
    """Parse units from single item html.

    Args:
        bs4_object: BeautifulSoup object holding the content of a single item html

    Returns:
        Tuple of unit key and list of unit values
        example: "Liegenschaften/Organisationseinheiten (LOZ)", ["FGx", "FGy"]
    """
    table = bs4_object.find("table", {"id": "loz"})
    if not isinstance(table, Tag):
        msg = (
            f"Missing table with ID 'loz' in single item html.\n{bs4_object.prettify()}"
        )
        raise MExError(msg)
    column_headers = table.find_all("th")
    key = str(column_headers[0].string)
    value = [str(c.string) for c in column_headers[1:]]
    return key, value
