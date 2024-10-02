from collections.abc import Generator, Iterable
from urllib.parse import urljoin

from mex.common.exceptions import MExError
from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models.person import LDAPPersonWithQuery
from mex.common.ldap.transform import analyse_person_string
from mex.common.logging import watch
from mex.extractors.confluence_vvt.connector import ConfluenceVvtConnector
from mex.extractors.confluence_vvt.models.source import ConfluenceVvtSource
from mex.extractors.confluence_vvt.parse_html import parse_data_html_page
from mex.extractors.settings import Settings


@watch
def fetch_all_data_page_ids() -> Generator[str, None, None]:
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
        raise MExError("Pagination limit reached to fetch all data pages list")


@watch
def fetch_all_pages_data(
    page_ids: Iterable[str],
) -> Generator[ConfluenceVvtSource, None, None]:
    """Fetch data from data pages.

    Args:
        page_ids: Iterable of ids of the pages to extract data from

    Settings:
        url: Confluence base url

    Returns:
        Generator for ConfluenceVvtSource items
    """
    connector = ConfluenceVvtConnector.get()
    settings = Settings.get()

    for page_id in page_ids:
        response = connector.session.get(
            urljoin(
                settings.confluence_vvt.url,
                f"rest/api/content/{page_id}?expand=body.view",
            ),
        )
        response.raise_for_status()
        json_data = response.json()

        html_body = json_data["body"]["view"]["value"]
        title = json_data["title"]
        if parsed_tuple := parse_data_html_page(html_body):
            (
                abstract,
                lead_author_names,
                lead_author_oes,
                deputy_author_names,
                deputy_author_oes,
                collaborating_author_names,
                collaborating_author_oes,
                interne_vorgangsnummer,
            ) = parsed_tuple
            yield ConfluenceVvtSource(
                abstract=abstract,
                identifier=page_id,
                title=title,
                contact=lead_author_names,
                identifier_in_primary_source=interne_vorgangsnummer,
                involved_person=lead_author_names
                + deputy_author_names
                + collaborating_author_names,
                responsible_unit=lead_author_oes,
                theme="https://mex.rki.de/item/theme-1",
                involved_unit=lead_author_oes
                + deputy_author_oes
                + collaborating_author_oes,
                had_primary_source="confluence-vvt",
            )


@watch
def extract_confluence_vvt_authors(
    confluence_vvt_sources: Iterable[ConfluenceVvtSource],
) -> Generator[LDAPPersonWithQuery, None, None]:
    """Extract LDAP persons with their query string for confluence-vvt authors.

    Args:
        confluence_vvt_sources: confluence-vvt sources

    Returns:
        Generator for LDAP persons with query
    """
    ldap = LDAPConnector.get()
    seen = set()
    for source in confluence_vvt_sources:
        for author in (
            *source.contact,
            *source.involved_person,
        ):
            if author in seen:
                continue
            seen.add(author)
            for name in analyse_person_string(author):
                persons = list(ldap.get_persons(name.surname, name.given_name))
                if len(persons) == 1 and persons[0].objectGUID:
                    yield LDAPPersonWithQuery(person=persons[0], query=author)
