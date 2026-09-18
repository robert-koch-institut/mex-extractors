from dagster import AssetExecutionContext, asset

from mex.common.cli import entrypoint
from mex.common.models import (
    AccessPlatformMapping,
    ExtractedAccessPlatform,
    ExtractedOrganization,
    ExtractedResource,
    ExtractedResourceSeries,
    ResourceMapping,
    ResourceSeriesMapping,
)
from mex.extractors.assets import load_yaml
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.seq_repo.extract import (
    extract_sources,
)
from mex.extractors.seq_repo.model import SeqRepoSource
from mex.extractors.seq_repo.transform import (
    transform_seq_repo_access_platform_to_extracted_access_platform,
    transform_seq_repo_resource_to_extracted_resource,
    transform_seq_repo_resource_to_extracted_resource_series,
)
from mex.extractors.settings import ExtractorsSettings
from mex.extractors.sinks import load


@asset(group_name="seq_repo")
def seq_repo_sources() -> list[SeqRepoSource]:
    """Extract sources from seq-repo."""
    return extract_sources()


@asset(group_name="seq_repo")
def seq_repo_extracted_access_platform() -> ExtractedAccessPlatform:
    """Extract access platform from seq-repo."""
    settings = ExtractorsSettings.get()
    access_platform_mapping = AccessPlatformMapping.model_validate(
        load_yaml(f"{settings.seq_repo.mapping_path}/access-platform.yaml")
    )
    mex_access_platform = (
        transform_seq_repo_access_platform_to_extracted_access_platform(
            access_platform_mapping,
        )
    )
    load([mex_access_platform])
    return mex_access_platform


@asset(group_name="seq_repo")
def seq_repo_extracted_resource_series(
    seq_repo_sources: list[SeqRepoSource],
    seq_repo_extracted_access_platform: ExtractedAccessPlatform,
    extracted_organization_rki: ExtractedOrganization,
) -> list[ExtractedResourceSeries]:
    """Extract resource series from seq-repo."""
    settings = ExtractorsSettings.get()
    resource_series_mapping = ResourceSeriesMapping.model_validate(
        load_yaml(f"{settings.seq_repo.mapping_path}/resource-series.yaml")
    )

    mex_resource_series = transform_seq_repo_resource_to_extracted_resource_series(
        resource_series_mapping,
        seq_repo_sources,
        seq_repo_extracted_access_platform,
        extracted_organization_rki,
    )
    load(mex_resource_series)
    return mex_resource_series


@asset(group_name="seq_repo", metadata={"entity_type": "resource"})
def seq_repo_resources(
    context: AssetExecutionContext,
    seq_repo_sources: list[SeqRepoSource],
    seq_repo_extracted_access_platform: ExtractedAccessPlatform,
    seq_repo_extracted_resource_series: list[ExtractedResourceSeries],
    extracted_organization_rki: ExtractedOrganization,
) -> list[ExtractedResource]:
    """Extract resources from seq-repo."""
    settings = ExtractorsSettings.get()
    resource_mapping = ResourceMapping.model_validate(
        load_yaml(f"{settings.seq_repo.mapping_path}/resource.yaml")
    )

    resources = transform_seq_repo_resource_to_extracted_resource(
        seq_repo_sources,
        seq_repo_extracted_access_platform,
        seq_repo_extracted_resource_series,
        resource_mapping,
        extracted_organization_rki,
    )
    load(resources)
    context.add_output_metadata({"num_items": len(resources)})
    return resources


@entrypoint()
def run() -> None:  # pragma: no cover
    """Run the seq-repo extractor job in-process."""
    run_job_in_process("seq_repo")
