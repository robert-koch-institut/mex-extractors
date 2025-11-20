from pydantic import Field, SecretStr

from mex.common.models import BaseModel
from mex.common.types import AssetsPath


class ConsentMailerSettings(BaseModel):
    """Settings definition class for the consent mailer."""

    backend_fetch_chunk_size: int = Field(
        1,
        description="The chunk size for fetching against the backend.",
    )
    mailpit_api_url: str = Field(
        "http://127.0.0.1:8025",
        description="The url to the api endpoint for mailpit. USED FOR TESTS ONLY!",
    )
    mailpit_api_user: SecretStr = Field(
        SecretStr("user"),
        description="The username used for Basic HTTP auth against the mailpit api."
        " USED FOR TESTS ONLY!",
    )
    mailpit_api_password: SecretStr = Field(
        SecretStr("password"),
        description="The password used for Basic HTTP auth against the mailpit api."
        " USED FOR TESTS ONLY!",
    )
    schedule: str | None = Field(
        None,
        description="A valid cron string defining when to run the consent mailer",
    )
    smtp_server: str = Field(
        "localhost:1025",
        description="Address and port (<address>:<port>) of the SMTP server to use for"
        " the consent mailer.",
    )
    template_path: AssetsPath = Field(
        AssetsPath("mailings"),
        description=(
            "Path to the directory with the jinja template file containing the"
            " email body template, absolute path or relative to `assets_dir`."
        ),
    )
