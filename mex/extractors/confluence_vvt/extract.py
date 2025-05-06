from collections.abc import Generator, Iterable
from urllib.parse import urljoin

from mex.common.exceptions import MExError
from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models import LDAPPersonWithQuery
from mex.common.ldap.transform import analyse_person_string
from mex.common.logging import logger, watch
from mex.common.models import ActivityMapping
from mex.extractors.confluence_vvt.connector import ConfluenceVvtConnector
from mex.extractors.confluence_vvt.models import ConfluenceVvtPage
from mex.extractors.settings import Settings


@watch()
def fetch_all_vvt_pages_ids() -> Generator[str, None, None]:
    """Fetch all the ids for data pages.

    Settings:
        confluence_vvt.url: Confluence-vvt base url
        confluence_vvt.overview_page_id: page id of the overview page

    Raises:
        MExError: When the pagination limit is exceeded

    Returns:
        Generator for page IDs
    """
    connector = ConfluenceVvtConnector.get()
    settings = Settings.get()

    limit = 100
    for start in range(0, 10**6, limit):
        response = connector.session.get(
            urljoin(
                settings.confluence_vvt.url,
                f"rest/api/content/{settings.confluence_vvt.overview_page_id}"
                f"/child/page?limit={limit}&start={start}",
            )
        )
        response.raise_for_status()
        results = response.json()["results"]
        if not results:
            break
        for item in results:
            yield item["id"]
    else:
        msg = "Pagination limit reached to fetch all data pages list"
        raise MExError(msg)


@watch()
def get_page_data_by_id(
    page_ids: Iterable[str],
) -> Generator[ConfluenceVvtPage, None, None]:
    """Get confluence page data by its id.

    Args:
        page_ids: list of confluence page ids

    Returns:
        Generator of ConfluenceVvtPage
    """
    connector = ConfluenceVvtConnector.get()
    for page_id in page_ids:
        page_data = connector.get_page_by_id(page_id)
        if not page_data:
            continue
        yield page_data


def extract_confluence_vvt_authors(
    authors: list[str],
) -> list[LDAPPersonWithQuery]:
    """Extract LDAP persons with their query string for confluence-vvt authors.

    Args:
        authors: list of authors

    Returns:
        Generator for LDAP persons with query
    """
    ldap = LDAPConnector.get()
    seen = set()

    ldap_persons: list[LDAPPersonWithQuery] = []
    for author in authors:
        if author in seen:
            continue
        if "@" in author:
            continue
        seen.add(author)
        for name in analyse_person_string(author):
            persons = ldap.get_persons(
                surname=name.surname, given_name=name.given_name, limit=2
            )
            if len(persons) == 1 and persons[0].objectGUID:
                ldap_persons.append(
                    LDAPPersonWithQuery(person=persons[0], query=author)
                )
    return ldap_persons


def get_contact_from_page(
    page: ConfluenceVvtPage,
    confluence_vvt_activity_mapping: ActivityMapping,
) -> list[str]:
    """Get contact from confluence page.

    Args:
        page: confluence-vvt page
        confluence_vvt_activity_mapping: activity mapping for confluence-vvt

    Returns:
        list of contacts
    """
    contact = page.tables[0].get_value_row_by_heading(
        confluence_vvt_activity_mapping.contact[0].fieldInPrimarySource  # type: ignore[arg-type]
    )
    return contact.cells[0].get_texts()


def get_involved_persons_from_page(
    page: ConfluenceVvtPage,
    confluence_vvt_activity_mapping: ActivityMapping,
) -> list[str]:
    """Get involved persons from confluence page.

    Args:
        page: confluence-vvt page
        confluence_vvt_activity_mapping: activity mapping for confluence-vvt

    Returns:
        list of involved persons
    """
    return [
        person_text
        for person in confluence_vvt_activity_mapping.involvedPerson
        for person_text in (
            page.tables[0]
            .get_value_row_by_heading(person.fieldInPrimarySource)  # type: ignore[arg-type]
            .cells[0]
            .get_texts()
        )
    ]


def get_all_persons_from_all_pages(
    pages: list[ConfluenceVvtPage],
    confluence_vvt_activity_mapping: ActivityMapping,
) -> list[str]:
    """Get a list of all persons from all confluence pages.

    Args:
        pages: confluence-vvt page
        confluence_vvt_activity_mapping: activity mapping for confluence-vvt

    Returns:
        list of all persons on confluence page
    """
    all_persons_on_page = []
    for page in pages:
        try:
            contacts = get_contact_from_page(page, confluence_vvt_activity_mapping)
            involved_persons = get_involved_persons_from_page(
                page, confluence_vvt_activity_mapping
            )
            all_persons_on_page.extend(contacts)
            all_persons_on_page.extend(involved_persons)
        except ValueError as error:  # pragma: no cover
            logger.error("error getting all persons from page %s: %s", page.id, error)

    return all_persons_on_page


def get_responsible_unit_from_page(
    page: ConfluenceVvtPage,
    confluence_vvt_activity_mapping: ActivityMapping,
) -> list[str]:
    """Get responsible unit from confluence page.

    Args:
        page: confluence-vvt page
        confluence_vvt_activity_mapping: activity mapping for confluence-vvt

    Returns:
        list of responsible unit
    """
    responsible_units = page.tables[0].get_value_row_by_heading(
        confluence_vvt_activity_mapping.responsibleUnit[0]
        .fieldInPrimarySource.split("|")[0]  # type: ignore[union-attr]
        .strip()
    )
    return [
        unit.strip()
        for text in responsible_units.cells[1].get_texts()
        for unit in text.split(",")
    ]


def get_involved_units_from_page(
    page: ConfluenceVvtPage,
    confluence_vvt_activity_mapping: ActivityMapping,
) -> list[str]:
    """Get involved unit from confluence page.

    Args:
        page: confluence-vvt page
        confluence_vvt_activity_mapping: activity mapping for confluence-vvt

    Returns:
        list of involved unit
    """
    all_units: list[str] = []
    for involved_unit in confluence_vvt_activity_mapping.involvedUnit:
        if involved_unit == confluence_vvt_activity_mapping.involvedUnit[3]:
            # skipping it because its always empty and breaks things
            continue
        for text in (
            page.tables[0]
            .get_value_row_by_heading(
                involved_unit.fieldInPrimarySource.split("|")[0].strip()  # type: ignore[union-attr]
            )
            .cells[1]
            .get_texts()
        ):
            all_units.extend(unit.strip() for unit in text.split(","))
    return all_units


def get_all_units_from_all_pages(
    pages: list[ConfluenceVvtPage],
    confluence_vvt_activity_mapping: ActivityMapping,
) -> list[str]:
    """Get a list of all units from all confluence pages.

    Args:
        pages: all confluence-vvt page
        confluence_vvt_activity_mapping: activity mapping for confluence-vvt

    Returns:
        list of all units on a confluence page
    """
    all_units_on_page = []
    for page in pages:
        try:
            responsible_units = get_responsible_unit_from_page(
                page, confluence_vvt_activity_mapping
            )
            involved_units = get_involved_units_from_page(
                page, confluence_vvt_activity_mapping
            )
            all_units_on_page.extend(responsible_units)
            all_units_on_page.extend(involved_units)
        except ValueError as error:  # pragma: no cover
            logger.error("error getting all units from page %s: %s", page.id, error)

    return all_units_on_page
