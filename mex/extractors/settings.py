from pydantic import AnyUrl, Field, SecretStr
from pydantic_core import Url

from mex.common.settings import BaseSettings
from mex.common.types import AssetsPath
from mex.extractors.biospecimen.settings import BiospecimenSettings
from mex.extractors.blueant.settings import BlueAntSettings
from mex.extractors.confluence_vvt.settings import ConfluenceVvtSettings
from mex.extractors.consent_mailer.settings import ConsentMailerSettings
from mex.extractors.contact_point.settings import ContactPointSettings
from mex.extractors.datenkompass.settings import DatenkompassSettings
from mex.extractors.datscha_web.settings import DatschaWebSettings
from mex.extractors.endnote.settings import EndnoteSettings
from mex.extractors.ff_projects.settings import FFProjectsSettings
from mex.extractors.grippeweb.settings import GrippewebSettings
from mex.extractors.ifsg.settings import IFSGSettings
from mex.extractors.igs.settings import IGSSettings
from mex.extractors.international_projects.settings import InternationalProjectsSettings
from mex.extractors.odk.settings import ODKSettings
from mex.extractors.open_data.settings import OpenDataSettings
from mex.extractors.publisher.settings import PublisherSettings
from mex.extractors.seq_repo.settings import SeqRepoSettings
from mex.extractors.sumo.settings import SumoSettings
from mex.extractors.synopse.settings import SynopseSettings
from mex.extractors.system.settings import SystemSettings
from mex.extractors.voxco.settings import VoxcoSettings
from mex.extractors.wikidata.settings import WikidataSettings


class Settings(BaseSettings):
    """Settings definition class for extractors and related scripts."""

    all_filter_mapping_path: AssetsPath = Field(
        AssetsPath("mappings/__all__"),
        description=(
            "Path to the directory with the biospecimen mapping files containing the "
            "default values, absolute path or relative to `assets_dir`."
        ),
    )

    all_checks_path: AssetsPath = Field(
        AssetsPath("checks/__final__"),
        description="Path to the directory with checks config for each extractor.",
    )

    skip_extractors: list[str] = Field(
        [],
        description="Skip execution of these extractors in dagster",
        validation_alias="MEX_SKIP_EXTRACTORS",
    )
    drop_api_key: SecretStr = Field(
        SecretStr("dummy_admin_key"),
        description="Drop API key with admin access to call all GET endpoints",
        validation_alias="MEX_DROP_API_KEY",
    )
    drop_api_url: AnyUrl = Field(
        Url("http://localhost:8081/"),
        description="MEx drop API url.",
        validation_alias="MEX_DROP_API_URL",
    )
    schedule: str = Field(
        "0 0 * * *",
        description="A valid cron string defining when to run extractor jobs",
        validation_alias="MEX_SCHEDULE",
    )
    kerberos_user: str = Field(
        "user@domain.tld",
        description="Kerberos user to authenticate against MSSQL server.",
    )
    kerberos_password: SecretStr = Field(
        SecretStr("password"),
        description="Kerberos password to authenticate against MSSQL server.",
    )
    s3_endpoint_url: AnyUrl = Field(
        AnyUrl("https://s3"),
        description="The complete URL to use for the constructed client.",
    )
    s3_access_key_id: SecretStr = Field(
        SecretStr("s3_access_key_id"),
        description="The access key to use when creating the client.",
    )
    s3_secret_access_key: SecretStr = Field(
        SecretStr("s3_secret_access_key"),
        description="The secret key to use when creating the client.",
    )
    s3_bucket_key: str = Field(
        "s3_bucket",
        description="The S3 bucket where to store objects.",
    )
    biospecimen: BiospecimenSettings = BiospecimenSettings()
    blueant: BlueAntSettings = BlueAntSettings()
    confluence_vvt: ConfluenceVvtSettings = ConfluenceVvtSettings()
    consent_mailer: ConsentMailerSettings = ConsentMailerSettings()
    contact_point: ContactPointSettings = ContactPointSettings()
    datenkompass: DatenkompassSettings = DatenkompassSettings()
    datscha_web: DatschaWebSettings = DatschaWebSettings()
    endnote: EndnoteSettings = EndnoteSettings()
    ff_projects: FFProjectsSettings = FFProjectsSettings()
    grippeweb: GrippewebSettings = GrippewebSettings()
    ifsg: IFSGSettings = IFSGSettings()
    igs: IGSSettings = IGSSettings()
    international_projects: InternationalProjectsSettings = (
        InternationalProjectsSettings()
    )
    odk: ODKSettings = ODKSettings()
    open_data: OpenDataSettings = OpenDataSettings()
    publisher: PublisherSettings = PublisherSettings()
    seq_repo: SeqRepoSettings = SeqRepoSettings()
    sumo: SumoSettings = SumoSettings()
    synopse: SynopseSettings = SynopseSettings()
    system: SystemSettings = SystemSettings()
    voxco: VoxcoSettings = VoxcoSettings()
    wikidata: WikidataSettings = WikidataSettings()
