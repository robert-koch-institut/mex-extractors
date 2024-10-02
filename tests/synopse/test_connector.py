import pytest
from requests import HTTPError

from mex.extractors.synopse.connector import ReportServerConnector


@pytest.mark.integration
@pytest.mark.skip(reason="Report server is being deprecated")
def test_initialization() -> None:
    connector = ReportServerConnector.get()
    try:
        connector._check_availability()
    except HTTPError:
        pytest.fail("HTTPError : Connector initialization failed.")
