# mypy: ignore-errors

from mex.common.types import MergedOrganizationIdentifier
from mex.extractors.pipeline import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetKey,
    DagsterEventType,
    EventRecordsFilter,
    asset,
    asset_check,
)


@asset(group_name="blueant")
def asset1_get_blueant_organizations(
    context: AssetExecutionContext,
) -> dict[str, MergedOrganizationIdentifier]:
    """Returns blueant organizations."""
    data = {
        "Gemeinsamer Bundesausschuss": MergedOrganizationIdentifier(
            "dndYcmAjSS3oSktVgN0wDp"
        ),
        "University of Sierra Leone": MergedOrganizationIdentifier(
            "gwIU3ZoCvMbyZT9ZAwHqwy"
        ),
        "Auswärtiges Amt": MergedOrganizationIdentifier("bNfOJgnBOwlvYZ2bY4e3yU"),
        "Deutsche Forschungsgemeinschaft": MergedOrganizationIdentifier(
            "bTnZAmfgUFH8CL00MJ645Z"
        ),
        "WHO Europe": MergedOrganizationIdentifier("eBof6ItMwgNiqxgHuk6ZxP"),
    }
    context.add_asset_metadata(metadata={"num_items": len(data)})
    return data


@asset(group_name="blueant")
def asset2_get_blueant_organizations_names(
    context: AssetExecutionContext,
    asset1_get_blueant_organizations: dict[str, MergedOrganizationIdentifier],
) -> list[str]:
    """Test asset follows directly after succesfull run of asset1."""
    organization_names = list(asset1_get_blueant_organizations.keys())
    context.add_asset_metadata(metadata={"num_items": len(organization_names)})
    return organization_names


@asset(group_name="blueant")
def asset_1_num_items(context: AssetExecutionContext) -> int | None:
    """Individual and independent asset for checking details of previous runs."""
    instance = context.instance
    event = instance.get_latest_materialization_event(
        AssetKey("asset1_get_blueant_organizations")
    )

    if event is None:
        context.log.warning(
            "No previous materialization found for asset1_get_blueant_organizations."
        )
        previous_num_items = None
    else:
        materialization = event.asset_materialization
        previous_num_items = materialization.metadata.get("num_items")
        if previous_num_items:
            previous_num_items = previous_num_items.value

    return previous_num_items


@asset_check(asset="asset1_get_blueant_organizations")
def check_asset1_organization_name(
    asset1: dict[str, MergedOrganizationIdentifier],
) -> AssetCheckResult:
    """Checks whether the returned organization names are correct."""
    expected_keys = {
        "Gemeinsamer Bundesausschuss",
        "University of Sierra Leone",
        "Auswärtiges Amt",
        "Deutsche Forschungsgemeinschaft",
        "WHO Europe",
    }
    passed = set(asset1.keys()) == expected_keys
    return AssetCheckResult(passed=passed)


@asset_check(asset="asset1_get_blueant_organizations")
def check_asset1_metadata(context: AssetCheckExecutionContext) -> AssetCheckResult:
    """Check for asset1_get_blueant_organizations() metadata values."""
    event = context.instance.get_latest_materialization_event(
        AssetKey("asset1_get_blueant_organizations")
    )
    previous_value = event.asset_materialization.metadata["num_items"].value
    current_value = 5

    passed = current_value == previous_value

    return AssetCheckResult(
        passed=passed,
        metadata={
            "previous": previous_value,
            "current": current_value,
        },
    )


@asset_check(asset="asset1_get_blueant_organizations", blocking=True)
def check_num_items_threshold(context: AssetCheckExecutionContext) -> AssetCheckResult:
    """Check for asset1_get_blueant_organizations() a certain threshold."""
    event = context.instance.get_latest_materialization_event(
        AssetKey("asset1_get_blueant_organizations")
    )
    num_items = event.asset_materialization.metadata["num_items"].value

    passed = num_items >= 5  # noqa: PLR2004

    return AssetCheckResult(
        passed=passed,
        metadata={"num_items": num_items, "threshold": 5},
        severity=AssetCheckSeverity.ERROR,
    )


@asset_check(asset="asset1_get_blueant_organizations", blocking=True)
def check_asset1_num_items_last_5_avg(
    context: AssetCheckExecutionContext,
) -> AssetCheckResult:
    """Checking the avg of last 5 materilizations of asset1. Stops pipeline if Error."""
    events = context.instance.get_event_records(
        EventRecordsFilter(
            event_type=DagsterEventType.ASSET_MATERIALIZATION,
            asset_key=AssetKey("asset1_get_blueant_organizations"),
        ),
        limit=5,
    )

    num_items_values = [
        e.asset_materialization.metadata["num_items"].value
        for e in events
        if "num_items" in e.asset_materialization.metadata
    ]

    if num_items_values:
        avg_num_items = sum(num_items_values) / len(num_items_values)
    else:
        avg_num_items = 0

    passed = avg_num_items >= 5  # noqa: PLR2004

    return AssetCheckResult(
        passed=passed,
        severity=AssetCheckSeverity.ERROR,
        metadata={
            "num_items_avg_last_5": avg_num_items,
            "threshold": 5,
            "samples": num_items_values,
        },
    )
