import http.client
import logging

import requests

from mex.common.cli import entrypoint
from mex.extractors.pipeline.base import run_job_in_process
from mex.extractors.settings import Settings


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run all extractor jobs in-process."""
    import logging

    logging.basicConfig(level=logging.INFO)
    activate_http_summary_logging()
    run_job_in_process("all_extractors")
    dump_http_summary()


def activate_http_logging() -> None:
    http.client.HTTPConnection.debuglevel = 1

    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    requests_log = logging.getLogger("urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True


import re
import time
from collections import defaultdict
from urllib.parse import urlsplit

http_log = logging.getLogger("http.summary")
_stats = defaultdict(lambda: {"count": 0, "errors": 0, "total_ms": 0.0})

_original_send = requests.sessions.Session.send

# match trailing "/123" (numbers only at the end)
_TRAILING_ID_RE = re.compile(r"/\d+$")


def _normalize_path(path: str) -> str:
    if not path:
        return "/"
    return _TRAILING_ID_RE.sub("/{id}", path)


def _logging_send(self, request, **kwargs):
    start = time.perf_counter()
    parts = urlsplit(request.url)

    normalized_path = _normalize_path(parts.path)
    key = (parts.netloc, normalized_path)

    try:
        response = _original_send(self, request, **kwargs)
        elapsed_ms = (time.perf_counter() - start) * 1000

        _stats[key]["count"] += 1
        _stats[key]["total_ms"] += elapsed_ms

        return response

    except Exception:
        elapsed_ms = (time.perf_counter() - start) * 1000
        _stats[key]["count"] += 1
        _stats[key]["errors"] += 1
        _stats[key]["total_ms"] += elapsed_ms

        http_log.exception(
            "%s %s%s -> ERROR %.1fms",
            request.method,
            parts.netloc,
            normalized_path,
            elapsed_ms,
        )
        raise


def activate_http_summary_logging():
    requests.sessions.Session.send = _logging_send


def dump_http_summary():
    for (host, path), data in sorted(_stats.items()):
        avg = data["total_ms"] / data["count"]
        http_log.info(
            "summary host=%s url=%s count=%d errors=%d avg=%.1fms",
            host,
            path,
            data["count"],
            data["errors"],
            avg,
        )
