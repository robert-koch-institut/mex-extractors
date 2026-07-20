from mex.common.exceptions import MExError
from mex.extractors.drop import DropApiConnector
from mex.extractors.seq_repo.model import SeqRepoSource


def extract_sources() -> list[SeqRepoSource]:
    """Extract Seq Repo sources by loading data from source json file.

    Returns:
        List of Seq Repo resources
    """
    connector = DropApiConnector.get()
    files = connector.list_files("seq-repo")
    if len(files) != 1:
        msg = f"Expected exactly one seq-repo file, got {len(files)}"
        raise MExError(msg)
    data = connector.get_file("seq-repo", files[0])
    return [SeqRepoSource.model_validate(item) for item in data]
