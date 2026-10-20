# ruff: noqa: ANN401, D103, E501, PLW0108, T201, TC003  (throwaway script)
"""THROWAWAY: parity check for the mex-common 3.5 mapping helper refactoring.

Compares every mapping-accessing code block as it was on `main` ("old") with the
helper-based version on `feature/mx-2322-use-mapping-helpers` ("new"), evaluated
against the mappings configured for this machine (same settings / .env as the
extractors, so point it at the real mappings).

    uv run python scripts/mapping_helper_parity.py [-v]

Statuses:
    OK          old and new return the same value (or raise the same error type)
    OLD_RAISED  old code raised, new code returns something (old would have crashed)
    NEW_RAISED  new code raises where old did not            -> regression
    BOTH_RAISED both raise, but different error types (both crash, fine)

    DIFF        both return, but different values            -> check if intended
    MISSING     mapping file not found / attribute not mapped in that file

Some DIFFs are the agreed "broadening" (all forValues instead of forValues[0],
all setValues instead of setValues[0], all rules/fields instead of index 0).
Read them, don't just count them.

Blatant copypasta on purpose. Delete after a successful run.
"""

import sys
from collections import Counter
from collections.abc import Callable, Iterable
from typing import Any

from mex.common.models import (
    AccessPlatformMapping,
    ActivityMapping,
    BibliographicResourceMapping,
    ConsentMapping,
    DistributionMapping,
    OrganizationMapping,
    ResourceMapping,
    ResourceSeriesMapping,
    VariableGroupMapping,
    VariableMapping,
)
from mex.common.types import Language
from mex.extractors.assets import glob_files, load_yaml
from mex.extractors.settings import ExtractorsSettings

VERBOSE = "-v" in sys.argv
PROBE = "__parity_probe_no_match__"
SKIP = "<<continue>>"
STATS: Counter[str] = Counter()
PROBLEMS: list[str] = []


# --------------------------------------------------------------------------- utils


def run(fn: Callable[[], Any]) -> tuple[bool, Any]:
    try:
        return True, fn()
    except Exception as error:  # noqa: BLE001
        return False, error


def dump(value: Any) -> Any:
    if isinstance(value, list):
        return [dump(v) for v in value]
    if hasattr(value, "model_dump"):
        return value.model_dump()
    return value


def check(label: str, old: Callable[[], Any], new: Callable[[], Any]) -> None:
    old_ok, old_value = run(old)
    new_ok, new_value = run(new)
    if old_ok and new_ok:
        status = "OK" if dump(old_value) == dump(new_value) else "DIFF"
    elif not old_ok and not new_ok:
        status = "OK" if type(old_value) is type(new_value) else "BOTH_RAISED"
    elif old_ok:
        status = "NEW_RAISED"
    else:
        status = "OLD_RAISED"
    STATS[status] += 1
    if status != "OK" or VERBOSE:
        line = f"{status:10} {label}\n           old: {old_value!r}\n           new: {new_value!r}"
        print(line)
        if status in ("DIFF", "NEW_RAISED"):
            PROBLEMS.append(line)


def missing(label: str) -> None:
    STATS["MISSING"] += 1
    print(f"MISSING    {label}")


def load(model: Any, path: str) -> Any:
    try:
        return model.model_validate(load_yaml(path))
    except Exception as error:  # noqa: BLE001
        missing(f"{path}: {error!r}"[:300])
        return None


def load_glob(model: Any, path: str, pattern: str) -> list[tuple[str, Any]]:
    try:
        files = glob_files(path, pattern)
    except Exception as error:  # noqa: BLE001
        missing(f"{path}/{pattern}: {error!r}"[:300])
        return []
    if not files:
        missing(f"{path}/{pattern}: no files")
    return [(f, m) for f in files if (m := load(model, f)) is not None]


def keys(field_list: Any, *extra: str | None) -> list[str | None]:
    """All forValues of all rules of all fields, plus a non-matching probe."""
    return [
        *dict.fromkeys(
            v for f in field_list for r in f.mappingRules for v in (r.forValues or [])
        ),
        PROBE,
        *extra,
    ]


def generic(
    label: str, mapping: Any, static: Iterable[str], mappable: Iterable[str]
) -> None:
    """Index-0 reads: F[0].mappingRules[0].setValues / .forValues."""
    for attr in static:
        field_list = getattr(mapping, attr)
        if not field_list:
            missing(f"{label}.{attr} (empty, old code would IndexError)")
            continue
        check(
            f"{label}.{attr} static",
            lambda fl=field_list: fl[0].mappingRules[0].setValues,
            lambda fl=field_list: fl.get_static_values(),
        )
    for attr in mappable:
        field_list = getattr(mapping, attr)
        if not field_list:
            missing(f"{label}.{attr} (empty, old code would IndexError)")
            continue
        check(
            f"{label}.{attr} mappable",
            lambda fl=field_list: fl[0].mappingRules[0].forValues or [],
            lambda fl=field_list: fl.get_mappable_values(),
        )


settings = ExtractorsSettings.get()

# --------------------------------------------------------------------- biospecimen

if m := load(ResourceMapping, f"{settings.biospecimen.mapping_path}/resource.yaml"):
    L = "biospecimen/resource"
    generic(
        L,
        m,
        static=[
            "conformsTo",
            "hasCodingSystem",
            "hasLegalBasis",
            "hasPersonalData",
            "language",
            "resourceCreationMethod",
            "theme",
        ],
        mappable=[],
    )
    for k in keys(m.resourceTypeGeneral, None):

        def old(k: Any = k) -> Any:
            if m.resourceTypeGeneral[0].mappingRules[0].forValues and (
                k in m.resourceTypeGeneral[0].mappingRules[0].forValues
            ):
                return m.resourceTypeGeneral[0].mappingRules[0].setValues
            return []

        def new(k: Any = k) -> Any:
            return m.resourceTypeGeneral.get_mapped_values(k) if k else []

        check(f"{L}.resourceTypeGeneral[{k!r}]", old, new)

    anonymization_pseudonymization_by_field = {
        rule.forValues[0]: rule.setValues
        for rule in m.anonymizationPseudonymization[0].mappingRules
        if rule.forValues and rule.setValues
    }
    for k in keys(m.anonymizationPseudonymization):
        check(
            f"{L}.anonymizationPseudonymization[{k!r}]",
            lambda k=k: anonymization_pseudonymization_by_field[k],
            lambda k=k: m.anonymizationPseudonymization.get_mapped_values(k),
        )
    access_restriction_by_zugriffsbeschraenkung = {
        rule.forValues[0]: rule.setValues
        for rule in m.accessRestriction[0].mappingRules
        if rule.forValues and rule.setValues
    }
    for k in keys(m.accessRestriction):
        check(
            f"{L}.accessRestriction[{k!r}]",
            lambda k=k: access_restriction_by_zugriffsbeschraenkung[k],
            lambda k=k: m.accessRestriction.get_mapped_values(k)[0],
        )

# ------------------------------------------------------------------------- blueant

if m := load(ActivityMapping, f"{settings.blueant.mapping_path}/activity.yaml"):
    L = "blueant/activity"
    activity_type_values_by_type_id = {
        for_value: mapping_rule.setValues
        for mapping_rule in m.activityType[0].mappingRules
        if mapping_rule.forValues
        for for_value in mapping_rule.forValues
    }
    for k in keys(m.activityType):
        check(
            f"{L}.activityType[{k!r}]",
            lambda k=k: activity_type_values_by_type_id.get(k, []),
            lambda k=k: m.activityType.get_mapped_values(k),
        )

# -------------------------------------------------------------------------- endnote

if m := load(ConsentMapping, f"{settings.endnote.mapping_path}/consent.yaml"):
    generic("endnote/consent", m, static=["hasConsentStatus"], mappable=[])

if m := load(
    BibliographicResourceMapping,
    f"{settings.endnote.mapping_path}/bibliographic-resource.yaml",
):
    L = "endnote/bibliographic-resource"
    language_by_language_field = {
        for_value: rule.setValues[0]
        for rule in m.language[0].mappingRules
        if rule.forValues and rule.setValues
        for for_value in rule.forValues
    }
    for k in keys(m.language, None):
        check(
            f"{L}.language[{k!r}]",
            lambda k=k: (
                language_by_language_field.get(k, Language["ENGLISH"])
                if k
                else Language["ENGLISH"]
            ),
            lambda k=k: (
                next(iter(m.language.get_mapped_values(k)), Language["ENGLISH"])
                if k
                else Language["ENGLISH"]
            ),
        )

    def build_access_restriction_by_custom6() -> dict[Any, Any]:
        access_restriction_by_custom6: dict[Any, Any] = {
            "default": m.accessRestriction[0].mappingRules[2].setValues,
        }
        access_restriction_by_custom6.update(
            {
                for_value: rule.setValues
                for rule in m.accessRestriction[0].mappingRules
                if rule.forValues and rule.setValues
                for for_value in rule.forValues
            }
        )
        return access_restriction_by_custom6

    for k in keys(m.accessRestriction, None):
        check(
            f"{L}.accessRestriction[{k!r}]",
            lambda k=k: build_access_restriction_by_custom6().get(
                k, build_access_restriction_by_custom6()["default"]
            ),
            lambda k=k: next(
                iter(m.accessRestriction.get_mapped_values(k) if k else []),
                m.accessRestriction.get_static_values(),
            ),
        )

    def build_bibliographical_resource_type_by_ref_type() -> dict[Any, Any]:
        bibliographical_resource_type_by_ref_type = {
            "default": (m.bibliographicResourceType[0].mappingRules[2].setValues[0])
            if m.bibliographicResourceType[0].mappingRules[2].setValues
            else None
        }
        bibliographical_resource_type_by_ref_type.update(
            {
                for_value: rule.setValues[0]
                for rule in m.bibliographicResourceType[0].mappingRules
                if rule.forValues and rule.setValues
                for for_value in rule.forValues
            }
        )
        return bibliographical_resource_type_by_ref_type

    for k in keys(m.bibliographicResourceType):
        check(
            f"{L}.bibliographicResourceType[{k!r}] (old scalar wrapped in list)",
            lambda k=k: [
                build_bibliographical_resource_type_by_ref_type().get(
                    k, build_bibliographical_resource_type_by_ref_type()["default"]
                )
            ],
            lambda k=k: (
                m.bibliographicResourceType.get_mapped_values(k)
                or m.bibliographicResourceType.get_static_values()
            ),
        )

# ---------------------------------------------------------------------- ff_projects

if m := load(ActivityMapping, f"{settings.ff_projects.mapping_path}/activity.yaml"):
    L = "ff_projects/activity"
    for k in keys(m.activityType):

        def old(k: Any = k) -> Any:
            if k in (m.activityType[0].mappingRules[0].forValues or ()):
                return m.activityType[0].mappingRules[0].setValues
            if k in (m.activityType[0].mappingRules[1].forValues or ()):
                return m.activityType[0].mappingRules[1].setValues
            return []

        check(
            f"{L}.activityType[{k!r}]",
            old,
            lambda k=k: m.activityType.get_mapped_values(k),
        )

# ------------------------------------------------------------------------ grippeweb

if m := load(
    AccessPlatformMapping, f"{settings.grippeweb.mapping_path}/access-platform.yaml"
):
    generic(
        "grippeweb/access-platform",
        m,
        static=["identifierInPrimarySource", "technicalAccessibility", "title"],
        mappable=["contact", "unitInCharge"],
    )

for path, m in load_glob(
    ResourceMapping, settings.grippeweb.mapping_path, "resource_*.yaml"
):
    L = f"grippeweb/{path}"
    generic(
        L,
        m,
        static=[
            "accessRestriction",
            "accrualPeriodicity",
            "anonymizationPseudonymization",
            "description",
            "documentation",
            "hasCodeValues",
            "hasLegalBasis",
            "hasPersonalData",
            "hasPurposeDescription",
            "keyword",
            "language",
            "meshId",
            "method",
            "methodDescription",
            "minTypicalAge",
            "populationCoverage",
            "resourceCreationMethod",
            "resourceTypeGeneral",
            "resourceTypeSpecific",
            "rights",
            "spatial",
            "start",
            "stateOfDataProcessing",
            "theme",
            "title",
        ],
        mappable=[
            "contact",
            "contributingUnit",
            "contributor",
            "publisher",
            "unitInCharge",
            "externalPartner",
        ],
    )

    def old(m: Any = m) -> Any:
        identifier_in_primary_source_mapping_rules = m.identifierInPrimarySource[
            0
        ].mappingRules[0]
        if set_values := identifier_in_primary_source_mapping_rules.setValues:
            return set_values
        return identifier_in_primary_source_mapping_rules.forValues[0]

    check(
        f"{L}.identifierInPrimarySource",
        old,
        lambda m=m: (
            m.identifierInPrimarySource.get_static_values()
            or m.identifierInPrimarySource.get_mappable_values()[0]
        ),
    )

if m := load(
    VariableGroupMapping, f"{settings.grippeweb.mapping_path}/variable-group.yaml"
):
    L = "grippeweb/variable-group"
    label_by_table_name = {
        mapping_rule.forValues[0]: mapping_rule.setValues
        for mapping_rule in m.label[0].mappingRules
    }
    for k in keys(m.label):
        check(
            f"{L}.label[{k!r}]",
            lambda k=k: label_by_table_name[k],
            lambda k=k: m.label.get_mapped_values(k),
        )

# ----------------------------------------------------------------------------- ifsg

if m := load(ResourceMapping, f"{settings.ifsg.mapping_path}/resource_parent.yaml"):
    generic(
        "ifsg/resource_parent",
        m,
        static=[
            "accessRestriction",
            "accrualPeriodicity",
            "alternativeTitle",
            "description",
            "hasLegalBasis",
            "hasPersonalData",
            "identifierInPrimarySource",
            "keyword",
            "language",
            "resourceCreationMethod",
            "resourceTypeGeneral",
            "rights",
            "spatial",
            "theme",
            "title",
        ],
        mappable=["contact", "unitInCharge"],
    )

if m := load(ResourceMapping, f"{settings.ifsg.mapping_path}/resource_state.yaml"):
    L = "ifsg/resource_state"
    generic(
        L,
        m,
        static=[
            "accessRestriction",
            "accrualPeriodicity",
            "description",
            "hasLegalBasis",
            "hasPersonalData",
            "keyword",
            "language",
            "resourceCreationMethod",
            "resourceTypeGeneral",
            "rights",
            "theme",
        ],
        mappable=["contact", "unitInCharge"],
    )
    bundesland_meldedaten_by_bundesland_id = {
        value.forValues[0]: value.setValues
        for value in m.alternativeTitle[0].mappingRules
    }
    documentation_by_bundesland_id = {
        value.forValues[0]: value.setValues for value in m.documentation[0].mappingRules
    }
    spatial_by_bundesland_id = None
    if m.spatial:
        spatial_by_bundesland_id = {
            value.forValues[0]: value.setValues
            for value in m.spatial[0].mappingRules
            if value.setValues
        }
    title_by_bundesland_id = {
        value.forValues[0]: value.setValues
        for value in m.title[0].mappingRules
        if value.setValues
    }
    check(
        f"{L} bundesland ids (loop driver)",
        lambda: list(bundesland_meldedaten_by_bundesland_id),
        lambda: list(dict.fromkeys(m.alternativeTitle.get_mappable_values())),
    )
    for k in keys(m.alternativeTitle):

        def old_spatial(k: Any = k) -> Any:
            if spatial_by_bundesland_id and k in spatial_by_bundesland_id:
                return spatial_by_bundesland_id[k]
            return []

        check(
            f"{L}.alternativeTitle[{k!r}]",
            lambda k=k: bundesland_meldedaten_by_bundesland_id[k],
            lambda k=k: m.alternativeTitle.get_mapped_values(k),
        )
        check(
            f"{L}.documentation[{k!r}]",
            lambda k=k: documentation_by_bundesland_id.get(k, []),
            lambda k=k: m.documentation.get_mapped_values(k),
        )
        check(
            f"{L}.spatial[{k!r}]",
            old_spatial,
            lambda k=k: m.spatial.get_mapped_values(k),
        )
        check(
            f"{L}.title[{k!r}]",
            lambda k=k: title_by_bundesland_id[k],
            lambda k=k: m.title.get_mapped_values(k),
        )

if m := load(ResourceMapping, f"{settings.ifsg.mapping_path}/resource_disease.yaml"):
    L = "ifsg/resource_disease"
    generic(
        L,
        m,
        static=[
            "accessRestriction",
            "accrualPeriodicity",
            "description",
            "hasCodingSystem",
            "hasLegalBasis",
            "hasPersonalData",
            "language",
            "resourceCreationMethod",
            "resourceTypeGeneral",
            "rights",
            "spatial",
            "theme",
        ],
        mappable=["contact", "unitInCharge"],
    )
    instrument_tool_or_apparatus_by_reference = {
        value.forValues[0]: value.setValues[0]
        for value in m.instrumentToolOrApparatus[0].mappingRules
    }
    for k in ["A=1", "B=1", "C=1", "D=1", "E=1"]:
        check(
            f"{L}.instrumentToolOrApparatus[{k!r}] (old scalar wrapped in list)",
            lambda k=k: [instrument_tool_or_apparatus_by_reference[k]],
            lambda k=k: m.instrumentToolOrApparatus.get_mapped_values(k),
        )
    bundesland_by_in_bundesland = {
        value.forValues[0]: value.setValues[0] for value in m.spatial[1].mappingRules
    }
    for k in keys(m.spatial):
        check(
            f"{L}.spatial[1][{k!r}] (old scalar wrapped in list)",
            lambda k=k: (
                [bundesland_by_in_bundesland[k]]
                if k in bundesland_by_in_bundesland
                else []
            ),
            lambda k=k: m.spatial.get_mapped_values(k),
        )

if m := load(VariableGroupMapping, f"{settings.ifsg.mapping_path}/variable-group.yaml"):
    L = "ifsg/variable-group"
    label_by_statement_area_group = {
        row.forValues[0]: row.setValues[0] for row in m.label[0].mappingRules
    }
    for k in keys(m.label):
        check(
            f"{L}.label[{k!r}] (old scalar wrapped in list)",
            lambda k=k: [label_by_statement_area_group[k]],
            lambda k=k: m.label.get_mapped_values(k),
        )

# ------------------------------------------------------------------------------ igs

if m := load(ResourceMapping, f"{settings.igs.mapping_path}/resource.yaml"):
    L = "igs/resource"
    generic(
        L,
        m,
        static=[
            "accessRestriction",
            "accrualPeriodicity",
            "alternativeTitle",
            "description",
            "documentation",
            "hasLegalBasis",
            "hasPurposeDescription",
            "healthCategory",
            "keyword",
            "language",
            "meshId",
            "method",
            "populationCoverage",
            "provenance",
            "qualityInformation",
            "resourceCreationMethod",
            "resourceTypeGeneral",
            "resourceTypeSpecific",
            "rights",
            "spatial",
            "start",
            "theme",
        ],
        mappable=["contact", "unitInCharge"],
    )
    check(
        f"{L}.contact (all rules)",
        lambda: [
            for_value
            for rule in m.contact[0].mappingRules
            if rule.forValues
            for for_value in rule.forValues
            if isinstance(for_value, str)
        ],
        lambda: m.contact.get_mappable_values(),
    )
    keywords_by_pathogen = {
        rule.forValues[0]: rule.setValues
        for rule in m.keyword[1].mappingRules
        if rule.forValues and rule.setValues
    }
    for k in keys(m.keyword):
        check(
            f"{L}.keyword[{k!r}]",
            lambda k=k: keywords_by_pathogen[k],
            lambda k=k: m.keyword.get_mapped_values(k),
        )
    title_by_pathogen = {
        rule.forValues[0]: rule.setValues
        for rule in m.title[0].mappingRules
        if rule.forValues and rule.setValues
    }
    for k in keys(m.title):
        check(
            f"{L}.title[{k!r}]",
            lambda k=k: title_by_pathogen.get(k, title_by_pathogen["[*]"]),
            lambda k=k: (
                m.title.get_mapped_values(k) or m.title.get_mapped_values("[*]")
            ),
        )

if m := load(
    AccessPlatformMapping, f"{settings.igs.mapping_path}/access-platform.yaml"
):
    generic(
        "igs/access-platform",
        m,
        static=[
            "description",
            "endpointDescription",
            "endpointType",
            "endpointURL",
            "identifierInPrimarySource",
            "landingPage",
            "technicalAccessibility",
            "title",
        ],
        mappable=["contact", "unitInCharge"],
    )

if m := load(VariableMapping, f"{settings.igs.mapping_path}/variable.yaml"):
    L = "igs/variable"
    for k in keys(m.dataType):
        check(
            f"{L}.dataType[1] contains {k!r}",
            lambda k=k: bool(
                m.dataType[1].mappingRules[0].forValues
                and k in m.dataType[1].mappingRules[0].forValues
            ),
            lambda k=k: k in m.dataType[1].get_mappable_values(),
        )

# ------------------------------------------------------------ international_projects

if m := load(
    ActivityMapping, f"{settings.international_projects.mapping_path}/activity.yaml"
):
    L = "international_projects/activity"
    for k in keys(m.activityType):

        def old(k: Any = k) -> Any:
            activity_type_from_mapping = m.activityType[0].mappingRules
            if k == activity_type_from_mapping[0].forValues[0]:
                return activity_type_from_mapping[0].setValues[0]
            if k == activity_type_from_mapping[1].forValues[0]:
                return activity_type_from_mapping[1].setValues[0]
            return activity_type_from_mapping[2].setValues[0]

        def new(k: Any = k) -> Any:
            return (
                m.activityType.get_mapped_values(k)
                or m.activityType.get_static_values()
                or []
            )[0]

        check(f"{L}.activityType[{k!r}]", old, new)

    def old_theme(key: Any) -> Any:
        themes_dict_from_mapping = {}
        default_theme_from_mapping = m.theme[0].mappingRules[0].setValues[0]
        for theme_item in m.theme:
            for mapping_rule in theme_item.mappingRules:
                if mapping_rule.forValues and mapping_rule.setValues:
                    themes_dict_from_mapping.update(
                        dict.fromkeys(mapping_rule.forValues, mapping_rule.setValues[0])
                    )
        if key and (theme := themes_dict_from_mapping.get(key)):
            return theme
        return default_theme_from_mapping

    def new_theme(key: Any) -> Any:
        default_theme_from_mapping = m.theme.get_static_values()[0]
        if key and (themes := m.theme.get_mapped_values(key)):
            return themes[0]
        return default_theme_from_mapping

    for k in keys(m.theme, None):
        check(f"{L}.theme[{k!r}]", lambda k=k: old_theme(k), lambda k=k: new_theme(k))

# ----------------------------------------------------------------------------- kvis

if m := load(ResourceMapping, f"{settings.kvis.mapping_path}/resource.yaml"):
    generic(
        "kvis/resource",
        m,
        static=[
            "accessRestriction",
            "accrualPeriodicity",
            "alternativeTitle",
            "description",
            "documentation",
            "hasCodingSystem",
            "hasLegalBasis",
            "hasPurposeDescription",
            "healthCategory",
            "identifierInPrimarySource",
            "keyword",
            "language",
            "method",
            "populationCoverage",
            "provenance",
            "resourceCreationMethod",
            "resourceTypeGeneral",
            "resourceTypeSpecific",
            "spatial",
            "start",
            "theme",
            "title",
        ],
        mappable=[
            "contact",
            "contributingUnit",
            "contributor",
            "externalPartner",
            "publisher",
            "unitInCharge",
        ],
    )

if m := load(VariableMapping, f"{settings.kvis.mapping_path}/variable.yaml"):
    L = "kvis/variable"
    valueset_variables_by_lookupfield = {
        rule.fieldInPrimarySource: rule.mappingRules[0].forValues for rule in m.valueSet
    }
    for field in ["FieldValue", "FieldValueLongText"]:
        check(
            f"{L}.valueSet[{field}]",
            lambda field=field: valueset_variables_by_lookupfield[field] or [],
            lambda field=field: m.valueSet.get_mappable_values(
                field_in_primary_source=field
            ),
        )

# ------------------------------------------------------------------------------ odk

for path, m in load_glob(ResourceMapping, settings.odk.mapping_path, "resource_*.yaml"):
    generic(
        f"odk/{path}",
        m,
        static=[
            "accessRestriction",
            "alternativeTitle",
            "description",
            "end",
            "hasLegalBasis",
            "healthCategory",
            "identifierInPrimarySource",
            "keyword",
            "language",
            "meshId",
            "method",
            "methodDescription",
            "numberOfUniqueIndividuals",
            "resourceCreationMethod",
            "resourceTypeGeneral",
            "resourceTypeSpecific",
            "rights",
            "spatial",
            "start",
            "theme",
            "title",
        ],
        mappable=[
            "contact",
            "contributingUnit",
            "externalPartner",
            "publisher",
            "unitInCharge",
            "wasGeneratedBy",
        ],
    )


# ------------------------------------------------------------------------ open_data

if m := load(
    DistributionMapping, f"{settings.open_data.mapping_path}/distribution.yaml"
):
    L = "open_data/distribution"
    generic(L, m, static=["accessRestriction"], mappable=[])
    for k in keys(m.license):
        check(
            f"{L}.license[{k!r}]",
            lambda k=k: (
                m.license[0].mappingRules[0].setValues
                if m.license[0].mappingRules[0].forValues
                and k in m.license[0].mappingRules[0].forValues
                else None
            ),
            lambda k=k: next(iter(m.license.get_mapped_values(k)), None),
        )

if m := load(ResourceMapping, f"{settings.open_data.mapping_path}/resource.yaml"):
    L = "open_data/resource"
    generic(
        L,
        m,
        static=[
            "accessRestriction",
            "anonymizationPseudonymization",
            "hasPersonalData",
            "theme",
        ],
        mappable=["contributingUnit"],
    )
    for k in keys(m.license):
        check(
            f"{L}.license[{k!r}]",
            lambda k=k: (
                m.license[0].mappingRules[0].setValues
                if m.license[0].mappingRules[0].forValues
                and k in m.license[0].mappingRules[0].forValues
                else None
            ),
            lambda k=k: next(iter(m.license.get_mapped_values(k)), None),
        )
    resource_type_general_lookup = {
        rule.forValues[0]: rule.setValues
        for rule in m.resourceTypeGeneral[0].mappingRules
        if rule.forValues
    }
    for k in keys(m.resourceTypeGeneral):
        check(
            f"{L}.resourceTypeGeneral[{k!r}]",
            lambda k=k: resource_type_general_lookup.get(k, []),
            lambda k=k: m.resourceTypeGeneral.get_mapped_values(k),
        )
    language_by_keyword = {
        rule.forValues[0]: rule.setValues[0]
        for rule in m.language[0].mappingRules
        if rule.forValues and rule.setValues
    }
    for k in keys(m.language):
        check(
            f"{L}.language[{k!r}] (old scalar wrapped in list)",
            lambda k=k: [language_by_keyword[k]],
            lambda k=k: m.language.get_mapped_values(k),
        )

# ------------------------------------------------------------------------- seq_repo

if m := load(
    AccessPlatformMapping, f"{settings.seq_repo.mapping_path}/access-platform.yaml"
):
    generic(
        "seq_repo/access-platform",
        m,
        static=[
            "alternativeTitle",
            "description",
            "endpointType",
            "identifierInPrimarySource",
            "landingPage",
            "technicalAccessibility",
            "title",
        ],
        mappable=["contact"],
    )

if m := load(ResourceMapping, f"{settings.seq_repo.mapping_path}/resource.yaml"):
    generic(
        "seq_repo/resource",
        m,
        static=[
            "accessRestriction",
            "accrualPeriodicity",
            "anonymizationPseudonymization",
            "description",
            "healthCategory",
            "keyword",
            "resourceCreationMethod",
            "resourceTypeGeneral",
            "resourceTypeSpecific",
            "rights",
            "stateOfDataProcessing",
            "theme",
        ],
        mappable=[],
    )

if m := load(
    ResourceSeriesMapping, f"{settings.seq_repo.mapping_path}/resource-series.yaml"
):
    generic(
        "seq_repo/resource-series", m, static=["description", "keyword"], mappable=[]
    )

# ----------------------------------------------------------------------------- sumo

if m := load(
    AccessPlatformMapping, f"{settings.sumo.mapping_path}/access-platform.yaml"
):
    generic(
        "sumo/access-platform",
        m,
        static=["identifierInPrimarySource", "technicalAccessibility", "title"],
        mappable=["contact", "unitInCharge"],
    )

if m := load(ActivityMapping, f"{settings.sumo.mapping_path}/activity.yaml"):
    generic(
        "sumo/activity",
        m,
        static=[
            "abstract",
            "activityType",
            "documentation",
            "identifierInPrimarySource",
            "shortName",
            "start",
            "theme",
            "title",
            "website",
        ],
        mappable=["contact", "externalAssociate", "involvedUnit", "responsibleUnit"],
    )

if m := load(ResourceMapping, f"{settings.sumo.mapping_path}/resource_feat-model.yaml"):
    generic(
        "sumo/resource_feat-model",
        m,
        static=[
            "accessRestriction",
            "accrualPeriodicity",
            "description",
            "hasCodingSystem",
            "hasPersonalData",
            "healthCategory",
            "meshId",
            "resourceCreationMethod",
            "resourceTypeGeneral",
            "resourceTypeSpecific",
            "spatial",
            "theme",
            "title",
        ],
        mappable=["contact", "contributingUnit", "unitInCharge"],
    )

if m := load(ResourceMapping, f"{settings.sumo.mapping_path}/resource_nokeda.yaml"):
    generic(
        "sumo/resource_nokeda",
        m,
        static=[
            "accessRestriction",
            "accrualPeriodicity",
            "description",
            "documentation",
            "hasPersonalData",
            "healthCategory",
            "meshId",
            "resourceCreationMethod",
            "resourceTypeGeneral",
            "resourceTypeSpecific",
            "rights",
            "spatial",
            "stateOfDataProcessing",
            "theme",
            "title",
        ],
        mappable=["contact", "contributingUnit", "externalPartner", "unitInCharge"],
    )

# -------------------------------------------------------------------------- synopse

if m := load(
    AccessPlatformMapping, f"{settings.synopse.mapping_path}/access-platform.yaml"
):
    generic(
        "synopse/access-platform",
        m,
        static=[
            "alternativeTitle",
            "description",
            "identifierInPrimarySource",
            "landingPage",
            "technicalAccessibility",
            "title",
        ],
        mappable=["contact", "unitInCharge"],
    )

if m := load(ActivityMapping, f"{settings.synopse.mapping_path}/activity.yaml"):
    L = "synopse/activity"
    generic(L, m, static=["activityType"], mappable=[])
    for k in keys(m.theme):
        check(
            f"{L}.theme[{k!r}]",
            lambda k=k: (
                m.theme[0].mappingRules[0].setValues
                if k in (m.theme[0].mappingRules[0].forValues or ())
                else m.theme[0].mappingRules[1].setValues
            ),
            lambda k=k: m.theme.get_mapped_values(k) or m.theme.get_static_values(),
        )

if m := load(ResourceMapping, f"{settings.synopse.mapping_path}/resource.yaml"):
    L = "synopse/resource"
    generic(
        L,
        m,
        static=[
            "healthCategory",
            "language",
            "resourceCreationMethod",
            "resourceTypeGeneral",
        ],
        mappable=["contact"],
    )
    for k in keys(m.theme):
        check(
            f"{L}.theme[{k!r}]",
            lambda k=k: (
                m.theme[0].mappingRules[0].setValues
                if k in (m.theme[0].mappingRules[0].forValues or ())
                else m.theme[0].mappingRules[1].setValues
            ),
            lambda k=k: m.theme.get_mapped_values(k) or m.theme.get_static_values(),
        )
    for k in keys(m.accessPlatform):
        check(
            f"{L}.accessPlatform contains {k!r}",
            lambda k=k: bool(
                m.accessPlatform[0].mappingRules[0].forValues
                and k in m.accessPlatform[0].mappingRules[0].forValues
            ),
            lambda k=k: k in m.accessPlatform.get_mappable_values(),
        )
    access_restriction_by_zugangsbeschraenkung = {
        for_value: rule.setValues
        for rule in m.accessRestriction[0].mappingRules
        if rule.forValues
        for for_value in rule.forValues
    }
    for k in keys(m.accessRestriction, ""):

        def old(k: Any = k) -> Any:
            if k and k in access_restriction_by_zugangsbeschraenkung:
                return access_restriction_by_zugangsbeschraenkung[k]
            return SKIP

        def new(k: Any = k) -> Any:
            if not (access_restrictions := m.accessRestriction.get_mapped_values(k)):
                return SKIP
            return access_restrictions[0]

        check(f"{L}.accessRestriction[{k!r}]", old, new)
    rights_by_dstyp_id = {
        rule.forValues[0]: rule.setValues for rule in m.rights[0].mappingRules
    }
    for k in keys(m.rights):
        check(
            f"{L}.rights[{k!r}]",
            lambda k=k: rights_by_dstyp_id[k],
            lambda k=k: m.rights.get_mapped_values(k),
        )

# ---------------------------------------------------------------------------- voxco

for path, m in load_glob(
    ResourceMapping, settings.voxco.mapping_path, "resource_*.yaml"
):
    generic(
        f"voxco/{path}",
        m,
        static=[
            "accessRestriction",
            "alternativeTitle",
            "anonymizationPseudonymization",
            "description",
            "healthCategory",
            "identifierInPrimarySource",
            "keyword",
            "language",
            "meshId",
            "method",
            "qualityInformation",
            "resourceCreationMethod",
            "resourceTypeGeneral",
            "resourceTypeSpecific",
            "rights",
            "spatial",
            "theme",
            "title",
        ],
        mappable=["contact", "externalPartner", "unitInCharge", "wasGeneratedBy"],
    )

# ------------------------------------------------------------------- confluence_vvt

if m := load(
    ActivityMapping, f"{settings.confluence_vvt.template_v1_mapping_path}/activity.yaml"
):
    check(
        "confluence_vvt/activity.documentation title",
        lambda: m.documentation[0].mappingRules[0].setValues[0].title,
        lambda: m.documentation.get_static_values()[0].title,
    )

# ------------------------------------------------------------------------- wikidata

if m := load(
    OrganizationMapping, f"{settings.wikidata.mapping_path}/organization.yaml"
):
    L = "wikidata/organization"
    wikidata_organization_ids_by_label = {
        value: rule.setValues
        for rule in m.identifierInPrimarySource[0].mappingRules
        if rule.setValues and rule.forValues
        for value in rule.forValues
    }
    for k in keys(m.identifierInPrimarySource):
        check(
            f"{L}.identifierInPrimarySource[{k!r}]",
            lambda k=k: wikidata_organization_ids_by_label.get(k),
            lambda k=k: next(
                iter(m.identifierInPrimarySource.get_mapped_values(k)), None
            ),
        )

# -------------------------------------------------------------------------- summary

print("\n" + "=" * 79)
print(", ".join(f"{k}: {v}" for k, v in sorted(STATS.items())))
if PROBLEMS:
    print(f"\n{len(PROBLEMS)} DIFF/NEW_RAISED to review (see above).")
    sys.exit(1)
print("no DIFF / NEW_RAISED")
