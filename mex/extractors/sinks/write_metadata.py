import datetime
import hashlib
import json
from importlib import metadata
from io import BytesIO, StringIO
from pathlib import Path

from packaging.version import Version

from mex.common.backend_api import BackendApiConnector
from mex.common.transform import MExEncoder
from mex.common.types import UTC


def build_directory_path(prefix: str | None) -> Path:
    """Build directory path that includes the mex-model major and minor version."""
    if prefix is None:
        prefix = ""
    mex_model_version = Version(metadata.version("mex-model"))
    return Path(f"{prefix}-{mex_model_version.major}.{mex_model_version.minor}")


def calculate_checksum(buffer: BytesIO | StringIO) -> str:
    """Calculate sha256 checksum of the buffer."""
    data = buffer.getvalue()

    if isinstance(data, str):
        data = data.encode("utf-8")  # handle StringIO type

    return hashlib.sha256(data).hexdigest()


def create_metadata_content(checksum: str) -> bytes:
    """Write metadata file."""
    backend_connector = BackendApiConnector.get()
    backend_version = backend_connector.system_status().version
    versions = {
        "mex-backend": backend_version,
        "mex-common": metadata.version("mex-common"),
        "mex-extractors": metadata.version("mex-extractors"),
        "mex-model": metadata.version("mex-model"),
    }
    payload = {
        "versions": versions,
        "sha256_checksum": checksum,
        "write_completed_at": datetime.datetime.now(tz=UTC).isoformat(),
    }
    payload_json = json.dumps(payload, sort_keys=True, cls=MExEncoder, indent=4)

    return payload_json.encode(encoding="utf-8")
