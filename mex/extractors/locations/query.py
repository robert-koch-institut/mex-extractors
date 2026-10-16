def query_content(query_string: str) -> str:
    """Query content from the EU vocabularies JSON file.

    Args:
        query_string (str): Id for which to query Wikidata for.

    Returns:
        str: completed query string
    """
    return """
        SELECT DISTINCT ?item ?itemLabel WHERE {
          SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],mul,en". }
          {
            SELECT DISTINCT ?item WHERE {
              {
                ?item p:P31 ?statement0.
                ?statement0 (ps:P31/(wdt:P279*)) wd:Q183.
              }
              UNION
              {
                ?item p:P31 ?statement1.
                ?statement1 (ps:P31/(wdt:P279*)) wd:Q1221156.
              }
              UNION
              {
                ?item p:P31 ?statement2.
                ?statement2 (ps:P31/(wdt:P279*)) wd:Q106658.
              }
              UNION
              {
                ?item p:P31 ?statement3.
                ?statement3 (ps:P31/(wdt:P279*)) wd:Q14784328.
              }
              UNION
              {
                ?item p:P31 ?statement4.
                ?statement4 (ps:P31/(wdt:P279*)) wd:Q22865.
              }
              UNION
              {
                ?item p:P31 ?statement5.
                ?statement5 (ps:P31/(wdt:P279*)) wd:Q116457956.
              }
            }
            LIMIT 2000
          }
        }
        Order by ?itemLabel
        """
