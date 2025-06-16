import contextlib
import re
from itertools import zip_longest
from typing import Any, cast

from bs4 import BeautifulSoup, ResultSet

INTERNE_VORGANGSNUMMER = "Interne Vorgangsnummer (Datenschutz)"
AUSFUEHRLICHE_BESCHREIBUNG_DES_VERFAHRENS = (
    "Ausführliche Beschreibung des Verfahrens / der Verarbeitung / der Studie"
)
VERANTWORTLICHE_STUDIENLEITERIN = "Verantwortliche(r) StudienleiterIn"
GGFS_VERTRETER_DER_DE_VERANTWORTLICHEN = "ggfs. Vertreter der / des Verantwortlichen"
MITARBEITENDE = "Mitarbeitende"
GGFS_NAME_DES_DER_DSB_DES_GEMEINSAM_VERANTWORTLICHEN_NACH_ART_26 = (
    "ggfs. Name des /der DSB des gemeinsam Verantwortlichen nach Art. 26 DSGVO "
    "(inkl. der Kontaktdaten)"
)
GGFS_AUFTRAGSVERARBEITER_NACH_ART = "ggfs. Auftragsverarbeiter nach Art. 28 DSGVO"
ZWECKE_DES_VORHABENS = "Zwecke des Vorhabens"


def parse_data_html_page(
    html: str,
) -> (
    tuple[
        str | list[str] | None,
        list[str],
        list[str],
        list[str],
        list[str],
        list[str],
        list[str],
        list[str] | Any,
    ]
    | None
):
    """Parse required data from html string.

    Args:
        html: Raw html in string format

    Returns:
        abstract, verantwortliche_studienleiterin, OE names and interne_vorgangsnummer
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find_all("table", {"class": "confluenceTable"})
    if len(table) < 2:  # noqa: PLR2004
        return None
    trs_table1 = table[0].find_all("tr")
    trs_table2 = table[1].find_all("tr")

    all_rows_data_table1 = get_row_data_for_all_rows(trs_table1)
    all_rows_data_table2 = get_row_data_for_all_rows(trs_table2, 2)

    abstract = all_rows_data_table1.get(AUSFUEHRLICHE_BESCHREIBUNG_DES_VERFAHRENS)
    zwecke_des_vorhabens = all_rows_data_table2.get(ZWECKE_DES_VORHABENS)

    if not abstract or len(abstract) < 1:
        abstract = None
    else:
        abstract = str(abstract).strip()
    if not zwecke_des_vorhabens or len(zwecke_des_vorhabens) < 1:
        zwecke_des_vorhabens = None

    if abstract and zwecke_des_vorhabens:
        str_ab = str(abstract)
        str_zw = str(zwecke_des_vorhabens)
        abstract = f"{str_ab}, {str_zw}"
    elif not abstract:
        abstract = None

    verantwortliche_studienleiterin = get_verantwortlichen(
        VERANTWORTLICHE_STUDIENLEITERIN,
        all_rows_data_table1,
    )

    ggfs_vertreter_verantwortlichen = get_verantwortlichen(
        GGFS_VERTRETER_DER_DE_VERANTWORTLICHEN,
        all_rows_data_table1,
    )

    mitarbeitende = get_verantwortlichen(
        MITARBEITENDE,
        all_rows_data_table1,
    )

    intnmr = get_interne_vorgangsnummer_from_all_rows_data(
        all_rows_data_table1.get(INTERNE_VORGANGSNUMMER)
    )

    return (
        abstract,
        *verantwortliche_studienleiterin,
        *ggfs_vertreter_verantwortlichen,
        *mitarbeitende,
        intnmr,
    )


def get_row_data_for_all_rows(
    table_rows: ResultSet[Any], min_ignorable_cols: int = 1
) -> dict[str, str | list[str]]:
    """Get all the data from the provided rows.

    Args:
        table_rows: Table rows ResultSet from bs4
        min_ignorable_cols: If row has multiple columns, number of columns below this
                            number will be ignored. Defaults to 1.

    Returns:
        structured dict of all the extracted data
    """
    all_rows_data: dict[str, str | list[str]] = {}
    for row in table_rows:
        current_row_headers = row.find_all("th")
        if current_row_headers:
            current_row_index = table_rows.index(row)
            next_row = table_rows[(current_row_index + 1)]
            next_row_values = next_row.find_all("td")
            header_text = current_row_headers[0].get_text().strip()

            if not next_row_values and "Interne Vorgangsnummer" in header_text:
                all_rows_data[INTERNE_VORGANGSNUMMER] = (
                    get_interne_vorgangsnummer_from_title(header_text)
                )

            elif not next_row_values:
                continue

            elif len(next_row_values) > min_ignorable_cols:
                all_cols_data = {}
                for key, value in zip(
                    current_row_headers, next_row_values, strict=False
                ):
                    current_row_all_cols_data = value.get_text(separator="\n").split(
                        "\n"
                    )
                    all_cols_data[key.get_text().strip()] = [
                        i
                        for i in get_clean_current_row_all_cols_data(
                            current_row_all_cols_data
                        )
                        if i
                    ]
                all_rows_data[header_text] = all_cols_data  # type: ignore[assignment]
            else:
                all_rows_data[header_text] = next_row_values[0].get_text()
    return all_rows_data


def get_clean_current_row_all_cols_data(
    current_row_all_cols_data: list[str],
) -> list[str]:
    """Get clean data for all cols in current row, removing all unwanted characters.

    Args:
        current_row_all_cols_data: List of all columns of current row

    Returns:
        list of cleaned strings for all columns of current row
    """
    cleaned_data = []
    for i in current_row_all_cols_data:
        cleaned_i = i.replace("\xa0", "")
        cleaned_i = cleaned_i.strip()
        cleaned_data.append(cleaned_i)
    return cleaned_data


def get_interne_vorgangsnummer_from_all_rows_data(
    intnmr_dict: Any | None | list[str],  # noqa: ANN401
) -> list[str] | Any:  # noqa: ANN401
    """Get Interne Vorgangsnummer from the extracted table.

    Args:
        intnmr_dict: Extracted dict or list of Interne Vorgangsnummer

    Returns:
        list of extracted Interne Vorgangsnummer
    """
    if not intnmr_dict:
        return []
    if isinstance(intnmr_dict, list):
        return intnmr_dict
    return intnmr_dict.get(INTERNE_VORGANGSNUMMER, [])


def get_interne_vorgangsnummer_from_title(
    interne_vorgangsnummer_title: str,
) -> list[str]:
    """Extract Interne Vorgangsnummer from the title row.

    Args:
        interne_vorgangsnummer_title: Interne Vorgangsnummer title

    Returns:
        list of extracted Interne Vorgangsnummer from the title
    """
    unwanted_elements = [
        "Interne",
        "Vorgangsnummer",
        "(Datenschutz):",
        "(Datenschutz):",
        "(Datenschutz)",
        "(Datenschutz).",
        "Vorgangsnummern",
        "(ImplantateregisterVst)",
        "Link",
        "zum",
    ]
    interne_vorgangsnummer = interne_vorgangsnummer_title.replace(
        "\n", ";"
    )  # replacing new lines with ; because below regex doesnt split on the basis of \n
    interne_vorgangsnummers = re.split(
        "; | |;|:|und", interne_vorgangsnummer.replace("\xa0", "")
    )

    for item in unwanted_elements:
        with contextlib.suppress(ValueError):
            interne_vorgangsnummers.remove(item)

    return [x for x in interne_vorgangsnummers if x]  # clean empty strings


def get_verantwortlichen(
    field_name: str, all_rows_data: dict[str, str | list[str]]
) -> tuple[list[str], list[str]]:
    """Get verantworlichen from the extracted all rows data.

    Args:
        field_name: Name of the field in the all_rows_data thats is to be extracted
        all_rows_data: All extracted rows data

    Returns:
        tuple of names and oes of verantworlicher(in)
    """
    names = []
    oes = []

    if field_data := cast("dict[str, str]", all_rows_data.get(field_name)):
        for name, oe in zip_longest(
            field_data[field_name],
            field_data["OE"],
        ):
            names.append(name)
            oes.append(oe)

    names_filtered: list[str] = list(filter(None, names))
    oes_filtered: list[str] = list(filter(None, oes))

    return names_filtered, oes_filtered
