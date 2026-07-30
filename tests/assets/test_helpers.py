from pathlib import Path

from mex.extractors.assets import load_yaml
from mex.extractors.assets.helpers import glob_files


def test_load_yaml() -> None:
    activity_filter_dict = load_yaml("mappings/__all__/activity_filter.yaml")
    assert activity_filter_dict == {
        "fields": [
            {
                "fieldInPrimarySource": "contact",
                "examplesInPrimarySource": None,
                "filterRules": [{"rule": None}],
                "comment": None,
            },
            {
                "fieldInPrimarySource": "end",
                "filterRules": [{"forValues": [" < 1890"], "rule": None}],
            },
            {
                "fieldInPrimarySource": "externalAssociate",
                "filterRules": [{"forValues": ["Erika Mustermann"], "rule": None}],
            },
            {
                "fieldInPrimarySource": "involvedUnit",
                "examplesInPrimarySource": None,
                "filterRules": [{"rule": None}],
            },
            {
                "fieldInPrimarySource": "responsibleUnit",
                "examplesInPrimarySource": None,
                "filterRules": [{"rule": None}, {"forValues": ["FG99"], "rule": None}],
            },
            {
                "fieldInPrimarySource": "start",
                "filterRules": [{"forValues": [" < 1890"], "rule": None}],
            },
        ]
    }


def test_glob_files() -> None:
    file_list = glob_files("mappings/__all__", "*.yaml")

    assert Path(file_list[0]).name == "activity_filter.yaml"
