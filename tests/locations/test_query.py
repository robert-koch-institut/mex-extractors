from mex.extractors.locations.query import build_wikidata_query


def test_build_wikidata_query() -> None:
    query = build_wikidata_query("Q12345")

    assert (
        query
        == """\
        SELECT DISTINCT ?item WHERE {
          ?item wdt:P31/wdt:P279* wd:Q12345 .
        }
    """
    )
