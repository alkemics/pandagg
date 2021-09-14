import json
from collections import Iterator
from os.path import join
from elasticsearch import Elasticsearch
from examples.imdb.conf import ES_HOST, ES_USE_AUTH, ES_PASSWORD, ES_USER, DATA_DIR

from pandagg.index import DeclarativeIndex, Action
from pandagg.mappings import Keyword, Text, Float, Nested, Integer


class Movies(DeclarativeIndex):
    name = "movies"
    mappings = {
        "dynamic": False,
        "properties": {
            "movie_id": Keyword(),
            "name": Text(fields={"raw": Keyword()}),
            "year": Integer(),
            "rank": Float(),
            "genres": Keyword(),
            "roles": Nested(
                properties={
                    "role": Keyword(),
                    "actor_id": Keyword(),
                    "gender": Keyword(),
                    "first_name": Text(fields={"raw": Keyword()}),
                    "last_name": Text(fields={"raw": Keyword()}),
                    "full_name": Text(fields={"raw": Keyword()}),
                }
            ),
            "directors": Nested(
                properties={
                    "director_id": Keyword(),
                    "first_name": Text(fields={"raw": Keyword()}),
                    "last_name": Text(fields={"raw": Keyword()}),
                    "full_name": Text(fields={"raw": Keyword()}),
                    "genres": Keyword(),
                }
            ),
            "nb_directors": Integer(),
            "nb_roles": Integer(),
        },
    }


def operations_iterator() -> Iterator[Action]:
    with open(join(DATA_DIR, "serialized.json"), "r") as f:
        for line in f.readlines():
            d = json.loads(line)
            yield {"_source": d, "_id": d["id"]}


if __name__ == "__main__":
    client_kwargs = {"hosts": [ES_HOST]}
    if ES_USE_AUTH:
        client_kwargs["http_auth"] = (ES_USER, ES_PASSWORD)
    client = Elasticsearch(**client_kwargs)

    movies = Movies(client)

    print("Index creation")
    movies.save()

    print("Write documents")
    movies.docs.bulk(
        actions=operations_iterator(), _op_type_overwrite="index"
    ).perform()
    movies.refresh()
