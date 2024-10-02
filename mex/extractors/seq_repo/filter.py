from collections.abc import Iterable
from datetime import datetime

from mex.extractors.seq_repo.model import SeqRepoSource


def filter_sources_on_latest_sequencing_date(
    seq_repo_sources: Iterable[SeqRepoSource],
) -> dict[str, SeqRepoSource]:
    """Filter sources on sequencing date, keeping only latest sequenced item.

    Args:
        seq_repo_sources: Seq Repo unfiltered extracted sources

    Returns:
        Filtered Seq Repo sources
    """
    unique_sources_with_latest_date: dict[str, SeqRepoSource] = {}
    for source in seq_repo_sources:
        identifier_in_primary_source = (
            f"{source.lims_sample_id}.{source.sequencing_platform}"
        )
        if identifier_in_primary_source not in unique_sources_with_latest_date:
            unique_sources_with_latest_date[identifier_in_primary_source] = source
        else:
            item_in_dict = unique_sources_with_latest_date[identifier_in_primary_source]
            item_in_dict_date = datetime.strptime(
                item_in_dict.sequencing_date, "%Y-%M-%d"
            )
            source_date = datetime.strptime(source.sequencing_date, "%Y-%M-%d")
            if source_date > item_in_dict_date:
                unique_sources_with_latest_date[identifier_in_primary_source] = source

    return unique_sources_with_latest_date
