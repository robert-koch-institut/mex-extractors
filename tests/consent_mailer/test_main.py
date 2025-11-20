import pprint
from typing import Any

import pytest
import requests

from mex.common.models import MergedPerson
from mex.common.types import AssetsPath
from mex.extractors.consent_mailer.main import (
    consent_mailer_send_emails,
)
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.settings import Settings


@pytest.mark.usefixtures("mocked_consent_backend_api_connector")
@pytest.mark.integration
def test_job() -> None:
    assert run_job_in_process("consent_mailer")


def _req_auth() -> tuple[str, str]:
    settings = Settings.get()
    return (
        settings.consent_mailer.mailpit_api_user.get_secret_value(),
        settings.consent_mailer.mailpit_api_password.get_secret_value(),
    )


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
        auth=_req_auth(),
    )


def _get_messages() -> Any:  # noqa: ANN401
    settings = Settings.get()

    response = requests.get(
        f"{settings.consent_mailer.mailpit_api_url}/mailpit/api/v1/messages",
        timeout=3,
        verify=_req_verify(),
        auth=_req_auth(),
    )

    if response.status_code != 200:
        print(f"DEBUG INFO: URL: {response.url}")  # noqa: T201
        print(f"DEBUG INFO: Status: {response.status_code}")  # noqa: T201
        print(f"DEBUG INFO: Body: {response.text}")  # noqa: T201

    return response.json()


@pytest.mark.requires_rki_infrastructure  # disabled on gh cli due to missing mailpit, stopgap MX-1993
def test_consent_mailer_send_emails() -> None:
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

    consent_mailer_send_emails(persons)

    envelop = _get_messages()
    assert envelop["total"] == 1
    assert all(
        all(x["Address"].endswith("rki.de") for x in message["To"])
        for message in envelop["messages"]
    )


@pytest.mark.requires_rki_infrastructure  # disabled on gh cli due to missing mailpit, stopgap MX-1993
def test_send_consent_no_emails_for_no_rki_persons() -> None:
    _delete_messages()

    envelop = _get_messages()
    assert envelop["total"] == 0, pprint.pformat(envelop)

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

    consent_mailer_send_emails(persons)

    envelop = _get_messages()
    assert envelop["total"] == 0, pprint.pformat(envelop)
