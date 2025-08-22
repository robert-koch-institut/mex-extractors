import base64
from typing import Any

import pytest
import requests

from mex.common.models import MergedPerson
from mex.common.types import AssetsPath
from mex.extractors.consent_mailer.main import (
    send_consent_emails_to_persons,
)
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.settings import Settings


@pytest.mark.usefixtures("mocked_consent_backend_api_connector")
def test_job() -> None:
    assert run_job_in_process("consent_mailer")


def _req_header() -> dict[str, str]:
    settings = Settings.get()
    return {
        "Authorization": "Basic "
        + base64.b64encode(
            f"{settings.consent_mailer.mailpit_api_user.get_secret_value()}:{settings.consent_mailer.mailpit_api_password.get_secret_value()}".encode()
        ).decode()
    }


def _req_verify() -> bool | str:
    settings = Settings.get()
    if isinstance(settings.verify_session, AssetsPath):
        return settings.verify_session._path.absolute().as_posix()
    return settings.verify_session


def _delete_messages() -> None:
    settings = Settings.get()
    requests.delete(
        f"{settings.consent_mailer.mailpit_api_url}/api/v1/messages",
        timeout=3,
        verify=_req_verify(),
        headers=_req_header(),
    )


def _get_messages() -> Any:  # noqa: ANN401
    settings = Settings.get()
    return requests.get(
        f"{settings.consent_mailer.mailpit_api_url}/api/v1/messages",
        timeout=3,
        verify=_req_verify(),
        headers=_req_header() | {"accept": "application/json"},
    ).json()


@pytest.mark.integration  # disabled on gh cli due to no mailHog available
def test_send_consent_emails_to_persons() -> None:
    _delete_messages()
    persons = [
        MergedPerson(
            identifier="efVXbcoxxMAiA5IqT1d11H",
            email=[
                "consent_private_mail@mail-provider.abc",
                "personT@rki.de",
            ],
            fullName=["Test Person"],
        ),
        MergedPerson(
            identifier="efVXbcoxxMAiA5IqT1d22H",
            email=[
                "NOconsent_private_mail@mail-provider.abc",
            ],
            fullName=["Dont send mails"],
        ),
    ]

    send_consent_emails_to_persons(persons)

    envelop = _get_messages()
    assert envelop["total"] == 1
    assert all(
        all(x["Address"].endswith("rki.de") for x in message["To"])
        for message in envelop["messages"]
    )


@pytest.mark.integration  # disabled on gh cli due to no mailHog available
def test_send_consent_no_emails_for_no_rki_persons() -> None:
    _delete_messages()

    persons = [
        MergedPerson(
            identifier="efVXbcoxxMAiA5IqT1d11H",
            email=[
                "consent_private_mail@mail-provider.abc",
                "personT@no-rki.de",
            ],
            fullName=["Test Person"],
        ),
        MergedPerson(
            identifier="efVXbcoxxMAiA5IqT1d22H",
            email=[
                "NOconsent_private_mail@mail-provider.abc",
            ],
            fullName=["Dont send mails"],
        ),
    ]

    send_consent_emails_to_persons(persons)

    envelop = _get_messages()
    assert envelop["total"] == 0
