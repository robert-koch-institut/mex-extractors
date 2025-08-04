import pytest
import requests

from mex.common.models import MergedPerson
from mex.extractors.consent_mailer.main import (
    send_consent_emails_to_persons,
)
from mex.extractors.pipeline import run_job_in_process

MAIL_HOG_API_URL = "http://localhost:8025"


@pytest.mark.usefixtures("mocked_consent_backend_api_connector")
def test_job() -> None:
    assert run_job_in_process("consent_mailer")


@pytest.mark.integration  # disabled on gh cli due to no mailHog available
def test_send_consent_emails_to_persons() -> None:
    requests.delete(f"{MAIL_HOG_API_URL}/api/v1/messages", timeout=3)

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

    envelop = requests.get(f"{MAIL_HOG_API_URL}/api/v2/messages", timeout=3).json()
    assert envelop["total"] == 1
    assert all(
        all(x["Domain"] == "rki.de" for x in message["To"])
        for message in envelop["items"]
    )


@pytest.mark.integration  # disabled on gh cli due to no mailHog available
def test_send_consent_no_emails_for_no_rki_persons() -> None:
    requests.delete(f"{MAIL_HOG_API_URL}/api/v1/messages", timeout=3)

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

    messages = requests.get(f"{MAIL_HOG_API_URL}/api/v2/messages", timeout=3).json()
    assert messages["total"] == 0
