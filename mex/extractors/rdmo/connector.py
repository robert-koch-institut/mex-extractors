from collections.abc import Generator
from functools import cache
from urllib.parse import urljoin

from mex.common.connector import HTTPConnector
from mex.common.exceptions import MExError
from mex.extractors.rdmo.models.question import RDMOOption, RDMOQuestion, RDMOValue
from mex.extractors.rdmo.models.source import RDMOSource
from mex.extractors.settings import Settings


class RDMOConnector(HTTPConnector):
    """Connector class to handle authentication and requesting the RDMO API."""

    def _set_url(self) -> None:
        """Set url of the host."""
        settings = Settings.get()
        self.url = settings.rdmo.url

    def _set_authentication(self) -> None:
        """Authenticate to the host."""
        settings = Settings.get()
        credentials = {
            "username": settings.rdmo.username.get_secret_value(),
            "password": settings.rdmo.password.get_secret_value(),
            "next": "/",
        }

        self.session.get(
            urljoin(self.url, "account/login/"),
            timeout=self.TIMEOUT,
        )
        response = self.session.post(
            urljoin(self.url, "account/login/"),
            data={
                **credentials,
                "csrfmiddlewaretoken": self.session.cookies["csrftoken"],
            },
            timeout=self.TIMEOUT,
            headers={"Referer": self.url},
        )
        if not response.ok:
            raise MExError(f"RDMO login failed: {response.status_code}")

    def get_sources(self) -> Generator[RDMOSource, None, None]:
        """Load RDMO sources by querying the RDMO API.

        Returns:
            Generator for RDMO sources
        """
        response = self.request("GET", "api/v1/projects/projects")
        for raw in response:
            yield RDMOSource.model_validate(raw)

    @cache  # noqa: B019
    def get_question_path(self, attribute: int) -> str:
        """Get the prefix-less UDI of an RDMO question, possibly from cache.

        Args:
            attribute: ID of the attribute

        Raises:
            MExError: If the attribute was not found

        Returns:
            str: Path of the question
        """
        response = self.request(
            "GET", f"api/v1/questions/questions/?attribute={attribute}"
        )
        for raw in response:
            question = RDMOQuestion.model_validate(raw)
            return question.uri.removeprefix(question.uri_prefix)
        raise MExError(f"No question found for {attribute=}")

    @cache  # noqa: B019
    def get_option_key(self, option: int) -> str:
        """Return the human-readable key for an option ID, possibly from cache.

        Args:
            option: ID of the option

        Returns:
            str: Readable option key
        """
        response = self.request("GET", f"api/v1/options/options/{option}")
        return RDMOOption.model_validate(response).key

    def get_question_answer_pairs(self, project: int) -> dict[str, str]:
        """Get a mapping of questions to answers for the given project.

        Lookup all answered questions for the given project and check the responses:
        If the questions has a fixed set of answer options return the human-readable
        option key, if the question is free-text return the the answer string.

        Args:
            project: ID of the project

        Returns:
            dict: Mapping of question paths to answers
        """
        response = self.request("GET", f"api/v1/projects/projects/{project}/values")
        pairs = {}
        for raw in response:
            value = RDMOValue.model_validate(raw)
            if value.text:
                answer = value.text
            elif value.option:
                answer = self.get_option_key(value.option)
            else:
                continue
            question = self.get_question_path(value.attribute)
            pairs[question] = answer
        return pairs
