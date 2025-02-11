from pydantic import AnyUrl, Field, SecretStr
from pydantic_core import Url

from mex.common.settings import BaseSettings
from mex.extractors.artificial.settings import ArtificialSettings
from mex.extractors.biospecimen.settings import BiospecimenSettings
from mex.extractors.blueant.settings import BlueAntSettings
from mex.extractors.confluence_vvt.settings import ConfluenceVvtSettings
from mex.extractors.datscha_web.settings import DatschaWebSettings
from mex.extractors.ff_projects.settings import FFProjectsSettings
from mex.extractors.grippeweb.settings import GrippewebSettings
from mex.extractors.ifsg.settings import IFSGSettings
from mex.extractors.international_projects.settings import InternationalProjectsSettings
from mex.extractors.odk.settings import ODKSettings
from mex.extractors.open_data.settings import OpenDataSettings
from mex.extractors.rdmo.settings import RDMOSettings
from mex.extractors.seq_repo.settings import SeqRepoSettings
from mex.extractors.sumo.settings import SumoSettings
from mex.extractors.synopse.settings import SynopseSettings
from mex.extractors.voxco.settings import VoxcoSettings


class Settings(BaseSettings):
    """Settings definition class for extractors and related scripts."""

    skip_extractors: list[str] = Field(
        [],
        description="Skip execution of these extractors in dagster",
        validation_alias="MEX_SKIP_EXTRACTORS",
    )
    skip_merged_items: list[str] = Field(
        ["MergedPrimarySource", "MergedConsent"],
        description="Skip merged items with these types",
        validation_alias="MEX_SKIP_MERGED_ITEMS",
    )
    skip_partners: list[str] = Field(
        ["test"],
        description="Skip projects with these external partners",
        validation_alias="MEX_SKIP_PARTNERS",
    )
    skip_units: list[str] = Field(
        ["IT", "PRAES", "ZV"],
        description="Skip projects with these responsible units",
        validation_alias="MEX_SKIP_UNITS",
    )
    skip_years_before: int = Field(
        1970,
        description="Skip projects conducted before this year",
        validation_alias="MEX_SKIP_YEARS_BEFORE",
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
    artificial: ArtificialSettings = ArtificialSettings()
    biospecimen: BiospecimenSettings = BiospecimenSettings()
    blueant: BlueAntSettings = BlueAntSettings()
    confluence_vvt: ConfluenceVvtSettings = ConfluenceVvtSettings()
    datscha_web: DatschaWebSettings = DatschaWebSettings()
    ff_projects: FFProjectsSettings = FFProjectsSettings()
    grippeweb: GrippewebSettings = GrippewebSettings()
    ifsg: IFSGSettings = IFSGSettings()
    international_projects: InternationalProjectsSettings = (
        InternationalProjectsSettings()
    )
    odk: ODKSettings = ODKSettings()
    open_data: OpenDataSettings = OpenDataSettings()
    rdmo: RDMOSettings = RDMOSettings()
    seq_repo: SeqRepoSettings = SeqRepoSettings()
    sumo: SumoSettings = SumoSettings()
    voxco: VoxcoSettings = VoxcoSettings()
    synopse: SynopseSettings = SynopseSettings()
