from collections.abc import Generator, Iterable

from mex.common.ldap.connector import LDAPConnector
from mex.common.ldap.models.person import LDAPPersonWithQuery
from mex.common.ldap.transform import analyse_person_string
from mex.common.logging import watch
from mex.common.wikidata.extract import search_organization_by_label
from mex.common.wikidata.models.organization import WikidataOrganization
from mex.extractors.datscha_web.connector import DatschaWebConnector
from mex.extractors.datscha_web.models.item import DatschaWebItem


@watch
def extract_datscha_web_items() -> Generator[DatschaWebItem, None, None]:
    """Load datscha source items by scraping datscha-web pages.

    Returns:
        Generator for datscha web items
    """
    connector = DatschaWebConnector.get()
    for item_url in connector.get_item_urls():
        item = connector.get_item(item_url)
        yield item


@watch
def extract_datscha_web_source_contacts(
    datscha_web_items: Iterable[DatschaWebItem],
) -> Generator[LDAPPersonWithQuery, None, None]:
    """Extract LDAP persons with their query string for datscha-web source contacts.

    Args:
        datscha_web_items: Datscha-web items

    Returns:
        Generator for LDAP persons with query
    """
    ldap = LDAPConnector.get()
    seen = set()
    for source in datscha_web_items:
        names = source.auskunftsperson
        if not names:
            continue
        if names in seen:
            continue
        seen.add(names)
        for name in analyse_person_string(names):
            persons = list(ldap.get_persons(name.surname, name.given_name))
            if len(persons) == 1 and persons[0].objectGUID:
                yield LDAPPersonWithQuery(person=persons[0], query=names)


def extract_datscha_web_organizations(
    datscha_web_items: Iterable[DatschaWebItem],
) -> dict[str, WikidataOrganization]:
    """Search and extract organization from wikidata.

    Args:
        datscha_web_items: Iterable of DatschaWebItem

    Returns:
        Dict with keys DatschaWebItem.Auftragsverarbeiter,
            DatschaWebItem.Empfaenger_der_Daten_im_Drittstaat, and
            DatschaWebItem.Empfaenger_der_verarbeiteten_uebermittelten_oder_offengelegten_Daten,
            and values: WikidataOrganization
    """
    partner_to_org_map = {}
    for item in datscha_web_items:
        for partner in item.get_partners():
            if partner and partner != "None":
                if organization := search_organization_by_label(partner):
                    partner_to_org_map[partner] = organization
    return partner_to_org_map
