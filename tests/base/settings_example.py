SETTINGS = {
    "index": {
        "analysis": {
            "analyzer": {
                "default": {
                    "char_filter": ["html_strip"],
                    "filter": ["lowercase", "asciifolding"],
                    "tokenizer": "standard",
                }
            }
        },
        "creation_date": "1570314471359",
        "number_of_replicas": "1",
        "number_of_shards": "5",
        "uuid": "7yy5q97eSVum8a54i-ZFXQ",
        "version": {"created": "2030399"},
    }
}
