from pathlib import Path

from dagster import AssetCheckContext, AssetCheckResult, asset_check
from mex.extractors.blueant.models.check import AssetCheck
from mex.extractors.utils import load_yaml


@asset_check(asset="extracted_blueant_activities", blocking=True)
def check_yaml_rules_exist(context: AssetCheckContext) -> AssetCheckResult:
    """Check if asset check rules for blueant activity are load."""
    path = Path(r"P:\Projects\mex\mex-assets\checks\__final__\blueant\activity.yaml")

    yaml_data = load_yaml(path)
    asset_check = AssetCheck(**yaml_data)
    num_rules = len(asset_check.rules)

    passed = num_rules > 0
    context.log.info("AssetCheck: passed={passed}, metadata={num_rules}").format(
        passed=num_rules, num_rules=num_rules
    )
    return AssetCheckResult(
        passed=passed,
        metadata={
            "num_rules": num_rules,
            "rule_types": [r.fail_if for r in asset_check.rules],
        },
    )
