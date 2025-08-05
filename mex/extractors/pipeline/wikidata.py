from dagster import asset
from mex.common.exceptions import MExError
from mex.common.models import ExtractedOrganization
from mex.extractors.wikidata.helpers import (
    get_wikidata_organization_by_id,
    get_wikidata_organization_ids_by_label,
)


@asset(group_name="default")
def extracted_organization_rki() -> ExtractedOrganization:
    """Extracts and Transforms RKI organization and loads result."""
    wikidata_organization_ids_by_label = get_wikidata_organization_ids_by_label()
    if (rki_id := wikidata_organization_ids_by_label.get("Robert Koch-Institut")) and (
        extracted_organization_rki := get_wikidata_organization_by_id(rki_id)
    ):
        return extracted_organization_rki
    msg = f"Robert Koch-Institut with ID {rki_id} not found in wikidata"
    raise MExError(msg)
