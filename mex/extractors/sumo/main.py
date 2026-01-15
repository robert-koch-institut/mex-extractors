from typing import Any

from dagster import asset

from mex.common.cli import entrypoint
from mex.common.ldap.transform import (
    transform_ldap_functional_accounts_to_extracted_contact_points,
    transform_ldap_persons_with_query_to_extracted_persons,
)
from mex.common.models import (
    AccessPlatformMapping,
    ActivityMapping,
    ExtractedAccessPlatform,
    ExtractedActivity,
    ExtractedOrganization,
    ExtractedOrganizationalUnit,
    ExtractedResource,
    ExtractedVariable,
    ExtractedVariableGroup,
    ResourceMapping,
)
from mex.common.types import (
    MergedContactPointIdentifier,
)
from mex.extractors.pipeline import run_job_in_process
from mex.extractors.primary_source.helpers import (
    get_extracted_primary_source_id_by_name,
)
from mex.extractors.settings import Settings
from mex.extractors.sinks import load
from mex.extractors.sumo.extract import (
    extract_cc1_data_model_nokeda,
    extract_cc1_data_valuesets,
    extract_cc2_aux_mapping,
    extract_cc2_aux_model,
    extract_cc2_aux_valuesets,
    extract_cc2_feat_projection,
    extract_ldap_contact_points_by_emails,
    extract_ldap_contact_points_by_name,
)
from mex.extractors.sumo.filter import filter_and_log_cc2_aux_model
from mex.extractors.sumo.models.cc1_data_model_nokeda import Cc1DataModelNoKeda
from mex.extractors.sumo.models.cc2_aux_model import Cc2AuxModel
from mex.extractors.sumo.models.cc2_feat_projection import Cc2FeatProjection
from mex.extractors.sumo.transform import (
    get_contact_merged_ids_by_emails,
    get_contact_merged_ids_by_names,
    transform_feat_projection_variable_to_mex_variable,
    transform_feat_variable_to_mex_variable_group,
    transform_model_nokeda_variable_to_mex_variable_group,
    transform_nokeda_aux_variable_to_mex_variable,
    transform_nokeda_aux_variable_to_mex_variable_group,
    transform_nokeda_model_variable_to_mex_variable,
    transform_resource_feat_model_to_mex_resource,
    transform_resource_nokeda_to_mex_resource,
    transform_sumo_access_platform_to_mex_access_platform,
    transform_sumo_activity_to_extracted_activity,
)
from mex.extractors.utils import load_yaml


@asset(group_name="sumo")
def sumo_extracted_access_platform(
    extracted_organizational_units: list[ExtractedOrganizationalUnit],
    extracted_organization_rki: ExtractedOrganization,
) -> ExtractedAccessPlatform:
    """Transform and load SUMO access platform and related LDAP actors."""
    settings = Settings.get()
    sumo_access_platform = AccessPlatformMapping.model_validate(
        load_yaml(settings.sumo.mapping_path / "access-platform.yaml"),
    )
    ldap_contact_points_access_platform = extract_ldap_contact_points_by_name(
        sumo_access_platform
    )
    mex_actors_access_platform = transform_ldap_persons_with_query_to_extracted_persons(
        ldap_contact_points_access_platform,
        get_extracted_primary_source_id_by_name("ldap"),
        extracted_organizational_units,
        extracted_organization_rki,
    )
    load(mex_actors_access_platform)

    contact_merged_ids_by_name = get_contact_merged_ids_by_names(
        mex_actors_access_platform
    )
    transformed_access_platform = transform_sumo_access_platform_to_mex_access_platform(
        sumo_access_platform,
        contact_merged_ids_by_name,
    )
    load([transformed_access_platform])

    return transformed_access_platform


@asset(group_name="sumo")
def sumo_merged_contact_ids_by_email(
    sumo_extracted_resources_nokeda: dict[str, Any],
    sumo_extracted_resources_feat: dict[str, Any],
) -> dict[str, MergedContactPointIdentifier]:
    """Load contacts related to resources and return them by their e-mail addresses."""
    ldap_contact_points_resources = extract_ldap_contact_points_by_emails(
        [
            ResourceMapping.model_validate(r)
            for r in [
                sumo_extracted_resources_nokeda,
                sumo_extracted_resources_feat,
            ]
        ]
    )
    mex_actors_resources = (
        transform_ldap_functional_accounts_to_extracted_contact_points(
            ldap_contact_points_resources,
            get_extracted_primary_source_id_by_name("ldap"),
        )
    )
    load(mex_actors_resources)
    return get_contact_merged_ids_by_emails(mex_actors_resources)


@asset(group_name="sumo")
def sumo_extracted_activity(
    sumo_merged_contact_ids_by_email: dict[str, MergedContactPointIdentifier],
) -> ExtractedActivity:
    """Extract, transform and load SUMO activity."""
    settings = Settings.get()
    sumo_activity = ActivityMapping.model_validate(
        load_yaml(settings.sumo.mapping_path / "activity.yaml"),
    )
    transformed_activity = transform_sumo_activity_to_extracted_activity(
        sumo_activity,
        sumo_merged_contact_ids_by_email,
    )
    load([transformed_activity])
    return transformed_activity


@asset(group_name="sumo")
def sumo_extracted_resources_nokeda() -> dict[str, Any]:
    """Extract Nokeda Resource from SUMO."""
    settings = Settings.get()
    return load_yaml(settings.sumo.mapping_path / "resource_nokeda.yaml")


@asset(group_name="sumo")
def sumo_extracted_resources_feat() -> dict[str, Any]:
    """Extract Resource feat from SUMO."""
    settings = Settings.get()
    return load_yaml(settings.sumo.mapping_path / "resource_feat-model.yaml")


@asset(group_name="sumo")
def sumo_extracted_cc1_data_model_nokeda() -> list[Cc1DataModelNoKeda]:
    """Extract Cc1 data model from SUMO."""
    return extract_cc1_data_model_nokeda()


@asset(group_name="sumo")
def sumo_extracted_cc2_aux_model() -> list[Cc2AuxModel]:
    """Extract Cc2 auxiliary model from SUMO."""
    sumo_cc2_aux_model = extract_cc2_aux_model()
    return filter_and_log_cc2_aux_model(sumo_cc2_aux_model)


@asset(group_name="sumo")
def sumo_extracted_cc2_feat_projection() -> list[Cc2FeatProjection]:
    """Extract Cc2 features from SUMO data."""
    return extract_cc2_feat_projection()


@asset(group_name="sumo")
def sumo_extracted_resource_nokeda(
    sumo_extracted_resources_nokeda: dict[str, Any],
    sumo_merged_contact_ids_by_email: dict[str, MergedContactPointIdentifier],
    extracted_organization_rki: ExtractedOrganization,
    sumo_extracted_activity: ExtractedActivity,
    sumo_extracted_access_platform: ExtractedAccessPlatform,
) -> ExtractedResource:
    """Transform and load extracted Nokeda Resource from SUMO."""
    mex_resource_nokeda = transform_resource_nokeda_to_mex_resource(
        ResourceMapping.model_validate(sumo_extracted_resources_nokeda),
        sumo_merged_contact_ids_by_email,
        extracted_organization_rki,
        sumo_extracted_activity,
        sumo_extracted_access_platform,
    )
    load([mex_resource_nokeda])
    return mex_resource_nokeda


@asset(group_name="sumo")
def sumo_extracted_resource_feat(
    sumo_extracted_resources_feat: dict[str, Any],
    sumo_merged_contact_ids_by_email: dict[str, MergedContactPointIdentifier],
    sumo_extracted_resource_nokeda: ExtractedResource,
    sumo_extracted_activity: ExtractedActivity,
    sumo_extracted_access_platform: ExtractedAccessPlatform,
) -> ExtractedResource:
    """Transform and load extracted SUMO Resource feat."""
    mex_resource_feat = transform_resource_feat_model_to_mex_resource(
        ResourceMapping.model_validate(sumo_extracted_resources_feat),
        sumo_merged_contact_ids_by_email,
        sumo_extracted_resource_nokeda,
        sumo_extracted_activity,
        sumo_extracted_access_platform,
    )
    load([mex_resource_feat])
    return mex_resource_feat


@asset(group_name="sumo")
def sumo_extracted_variable_groups_nokeda_aux(
    sumo_extracted_cc2_aux_model: list[Cc2AuxModel],
    sumo_extracted_resource_nokeda: ExtractedResource,
) -> list[ExtractedVariableGroup]:
    """Transform Nokeda auxiliary variables to MEx variable groups and load them."""
    mex_variable_groups_nokeda_aux = (
        transform_nokeda_aux_variable_to_mex_variable_group(
            sumo_extracted_cc2_aux_model,
            sumo_extracted_resource_nokeda,
        )
    )
    load(mex_variable_groups_nokeda_aux)
    return mex_variable_groups_nokeda_aux


@asset(group_name="sumo")
def sumo_extracted_variable_groups_nokeda(
    sumo_extracted_cc1_data_model_nokeda: list[Cc1DataModelNoKeda],
    sumo_extracted_resource_nokeda: ExtractedResource,
) -> list[ExtractedVariableGroup]:
    """Transform Nokeda variables to MEx variable groups and load them."""
    mex_variable_groups_model_nokeda = (
        transform_model_nokeda_variable_to_mex_variable_group(
            sumo_extracted_cc1_data_model_nokeda,
            sumo_extracted_resource_nokeda,
        )
    )
    load(mex_variable_groups_model_nokeda)
    return mex_variable_groups_model_nokeda


@asset(group_name="sumo")
def sumo_extracted_variable_group_feat(
    sumo_extracted_cc2_feat_projection: list[Cc2FeatProjection],
    sumo_extracted_resource_nokeda: ExtractedResource,
) -> list[ExtractedVariableGroup]:
    """Transform SUMO Resource feat to MEx variable groups and load them."""
    mex_variable_groups_feat = transform_feat_variable_to_mex_variable_group(
        sumo_extracted_cc2_feat_projection,
        sumo_extracted_resource_nokeda,
    )
    load(mex_variable_groups_feat)
    return mex_variable_groups_feat


@asset(group_name="sumo")
def sumo_extracted_variables_nokeda(
    sumo_extracted_cc1_data_model_nokeda: list[Cc1DataModelNoKeda],
    sumo_extracted_variable_groups_nokeda: list[ExtractedVariableGroup],
    sumo_extracted_resource_nokeda: ExtractedResource,
) -> list[ExtractedVariable]:
    """Transform Nokeda variables to extracted variables and load them."""
    sumo_cc1_data_valuesets = extract_cc1_data_valuesets()
    transformed_nokeda_model_variable = transform_nokeda_model_variable_to_mex_variable(
        sumo_extracted_cc1_data_model_nokeda,
        sumo_cc1_data_valuesets,
        sumo_extracted_variable_groups_nokeda,
        sumo_extracted_resource_nokeda,
    )
    load(transformed_nokeda_model_variable)
    return transformed_nokeda_model_variable


@asset(group_name="sumo")
def sumo_extracted_variables_nokeda_aux(
    sumo_extracted_cc2_aux_model: list[Cc2AuxModel],
    sumo_extracted_variable_groups_nokeda_aux: list[ExtractedVariableGroup],
    sumo_extracted_resource_nokeda: ExtractedResource,
) -> list[ExtractedVariable]:
    """Transform Nokeda aux variables to extracted variables and load them."""
    sumo_cc2_aux_mapping = extract_cc2_aux_mapping(sumo_extracted_cc2_aux_model)
    sumo_cc2_aux_valuesets = extract_cc2_aux_valuesets()

    sumo_extracted_variables_nokeda_aux = transform_nokeda_aux_variable_to_mex_variable(
        sumo_extracted_cc2_aux_model,
        sumo_cc2_aux_mapping,
        sumo_cc2_aux_valuesets,
        sumo_extracted_variable_groups_nokeda_aux,
        sumo_extracted_resource_nokeda,
    )
    load(sumo_extracted_variables_nokeda_aux)
    return sumo_extracted_variables_nokeda_aux


@asset(group_name="sumo")
def sumo_extracted_variables_feat_projection(
    sumo_extracted_cc2_feat_projection: list[Cc2FeatProjection],
    sumo_extracted_variable_group_feat: list[ExtractedVariableGroup],
    sumo_extracted_resource_feat: ExtractedResource,
) -> list[ExtractedVariable]:
    """Transform SUMO feat projection variables to extracted variables and load them."""
    transformed_feat_projection_variable = (
        transform_feat_projection_variable_to_mex_variable(
            sumo_extracted_cc2_feat_projection,
            sumo_extracted_variable_group_feat,
            sumo_extracted_resource_feat,
        )
    )
    load(transformed_feat_projection_variable)
    return transformed_feat_projection_variable


@entrypoint(Settings)
def run() -> None:  # pragma: no cover
    """Run the sumo extractor job in-process."""
    run_job_in_process("sumo")
