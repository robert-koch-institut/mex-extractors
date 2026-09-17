from typing import Final

from mex.model import VOCABULARY_JSON_BY_NAME

GERMAN_PREFLABEL_BY_CONCEPT_ID: Final[dict[str, str]] = {
    concept["identifier"]: pref_label["de"]
    for concepts in VOCABULARY_JSON_BY_NAME.values()
    for concept in concepts
    if (pref_label := concept.get("prefLabel"))
}
