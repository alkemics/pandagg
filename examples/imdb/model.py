from pandagg.document import DocumentSource, InnerDocSource
from pandagg.index import DeclarativeIndex
from pandagg.mappings import Keyword, Text, Float, Nested, Integer


class Role(InnerDocSource):
    role = Keyword()
    actor_id = Keyword()
    gender = Keyword()
    first_name = Text()
    last_name = Text()
    full_name = Text()


class Director(InnerDocSource):
    director_id = Keyword()
    first_name = Text()
    last_name = Text()
    full_name = Text()
    genres = Keyword()


class MovieEntry(DocumentSource):
    movie_id = Keyword()
    name = Text(fields={"raw": Keyword()})
    year = Integer()
    rank = Float()
    genres = Keyword()
    roles = Nested(Role)
    directors = Nested(Director)
    nb_directors = Integer()
    nb_roles = Integer()


class Movies(DeclarativeIndex):
    name = "movies"
    document = MovieEntry
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
