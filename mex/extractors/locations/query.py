def build_wikidata_query(region_type_id: str) -> str:
    """Query Wikidata items belonging to the specified region type..

    Args:
        region_type_id (str): Id for which to query Wikidata for.

    Returns:
        str: completed query string
    """
    return f"""\
        SELECT DISTINCT ?item WHERE {{
          ?item wdt:P31/wdt:P279* wd:{region_type_id} .
        }}
    """
