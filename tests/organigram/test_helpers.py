from mex.extractors.organigram.helpers import get_unit_merged_id_by_synonym


def test_get_unit_merged_id_by_synonym() -> None:
    merged_id = get_unit_merged_id_by_synonym("C1")
    # TODO(EH): mock get_extracted_unit_by_synonyms
    assert str(merged_id) == ""
