from collections.abc import Generator

from mex.common.logging import watch
from mex.extractors.open_data.connector import OpenDataConnector
from mex.extractors.open_data.models.source import ZenodoParentRecordSource


@watch
def get_parent_records() -> Generator[ZenodoParentRecordSource, None, None]:
    """Load Open Data sources by querying the Zenodo API.

    Get all records of  Zenodo community 'robertkochinstitut'

    Returns:
        Generator for Open Data sources

    contact_names = [creator["name"] for creator in item.metadata.creators].
    contributor_names = [
        contributor["name"] for contributor in item.metadata.contributors
    ].
    creation_date = "TODO"  # API-Call for oldest version.
    description = item.metadata.description  # TODO: Clean Up according to yaml.
    documentation = [
        identifier["identifier"]
        for identifier in item.metadata.related_identifiers
        if identifier["relation"] == "isDocumentedBy"
    ].
    doi = item.conceptdoi  # TODO: add "https://doi.org/".
    identifierInPrimarySource = item.conceptrecid.
    license = item.metadata.license.id.
    modified = item.modified.
    keyword = [word for word in item.metadata.keywords].
    language = item.metadata.language.
    title = item.metadata.title.
    """
    connector = OpenDataConnector()

    yield from connector.get_parent_sources()  ## for item in XY: yield item


def get_record_versions() -> Generator[ZenodoParentRecordSource, None, None]:
    """Fetch all the data from the parent resource.

    Args:
        conceptrecid: Iterable of parent record ids

    Raises:
        MExError: When the pagination limit is exceeded

    Returns:
        Generator for ZenodoParentRecordSource items

    contact_names = [creator["name"] for creator in item.metadata.creators].
    contributor_names = [
        contributor["name"] for contributor in item.metadata.contributors
    ].
    creation_date = item.created.
    description = item.metadata.description  # TODO: Clean Up according to yaml.
    distribution = [file["id"] for file in item.files].
    documentation = [
        identifier["identifier"]
        for identifier in item.metadata.related_identifiers
        if identifier["relation"] == "isDocumentedBy"
    ].
    doi = item.doi_url.
    identifierInPrimarySource = item.id.
    license = item.metadata.license.id.
    modified = item.modified.
    isPartOf = item.conceptrecid.
    keyword = [word for word in item.metadata.keywords].
    language = item.metadata.language.
    title = item.metadata.title.
    """
    connector = OpenDataConnector()

    yield from connector.get_parent_sources()  # for item in XY: yield item
