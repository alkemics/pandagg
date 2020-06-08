#!/usr/bin/env python
# coding: utf-8

"""Module to extract
"""

import logging
from os.path import join
import simplejson
import pandas as pd
import numpy as np

from examples.imdb.conf import DATA_DIR, OUTPUT_FILE_NAME

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class NpEncoder(simplejson.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)


if __name__ == "__main__":

    logging.info("LOADING TABLES")
    # tables imports
    reader_kwargs = {
        "encoding": "utf-8",
        "sep": ",",
        "quotechar": '"',
        "escapechar": "\\",
    }
    movies = pd.read_csv(join(DATA_DIR, "movies.csv"), index_col="id", **reader_kwargs)
    movies_genres = pd.read_csv(join(DATA_DIR, "movies_genres.csv"), **reader_kwargs)
    movies_directors = pd.read_csv(
        join(DATA_DIR, "movies_directors.csv"), **reader_kwargs
    )
    directors = pd.read_csv(
        join(DATA_DIR, "directors.csv"), index_col="id", **reader_kwargs
    )
    director_genres = pd.read_csv(
        join(DATA_DIR, "directors_genres.csv"), **reader_kwargs
    )
    roles = pd.read_csv(join(DATA_DIR, "roles.csv"), **reader_kwargs)
    actors = pd.read_csv(join(DATA_DIR, "actors.csv"), index_col="id", **reader_kwargs)

    # actors
    logging.info("SERIALIZE ACTORS")
    actor_roles = pd.merge(actors, roles, left_index=True, right_on="actor_id")
    actor_roles["serialized_roles"] = actor_roles.apply(
        lambda x: {
            "actor_id": x.actor_id,
            "first_name": x.first_name,
            "last_name": x.last_name,
            "full_name": "%s %s" % (x.first_name, x.last_name),
            "gender": x.gender,
            "role": x.role,
        },
        axis=1,
    )
    movie_serialized_actors = actor_roles.groupby("movie_id").serialized_roles.apply(
        list
    )

    # directors
    logging.info("SERIALIZE DIRECTORS")
    directors_grouped_genres = pd.DataFrame(
        director_genres.groupby("director_id").genre.apply(list)
    )
    movie_directors_extended = pd.merge(
        movies_directors, directors, left_on="director_id", right_index=True
    )
    movie_directors_extended = pd.merge(
        movie_directors_extended,
        directors_grouped_genres,
        how="left",
        left_on="director_id",
        right_index=True,
    )
    movie_directors_extended["serialized_directors"] = movie_directors_extended.apply(
        lambda x: {
            "director_id": x.director_id,
            "first_name": x.first_name,
            "last_name": x.last_name,
            "full_name": "%s %s" % (x.first_name, x.last_name),
            "genres": x.genre,
        },
        axis=1,
    )
    movie_serialized_directors = pd.DataFrame(
        movie_directors_extended.groupby("movie_id").serialized_directors.apply(list)
    )

    # movie genres
    logging.info("SERIALIZE MOVIE GENRES")
    movie_serialized_genres = movies_genres.groupby("movie_id").genre.apply(list)

    # merge
    logging.info("MERGE DATASETS")
    enriched_movies = pd.merge(
        movies, movie_serialized_actors, how="left", left_index=True, right_index=True
    )
    enriched_movies = pd.merge(
        enriched_movies,
        movie_serialized_directors,
        how="left",
        left_index=True,
        right_index=True,
    )
    enriched_movies = pd.merge(
        enriched_movies,
        movie_serialized_genres,
        how="left",
        left_index=True,
        right_index=True,
    )

    enriched_movies["nb_directors"] = enriched_movies.serialized_directors.apply(
        lambda x: len(x) if isinstance(x, list) else 0
    )
    enriched_movies["nb_roles"] = enriched_movies.serialized_roles.apply(
        lambda x: len(x) if isinstance(x, list) else 0
    )

    serialized = enriched_movies.apply(
        lambda x: {
            "movie_id": x.name,
            "name": x.loc["name"],
            "year": x.year,
            "genres": x.genre,
            "roles": x.serialized_roles,
            "nb_roles": x.nb_roles,
            "directors": x.serialized_directors,
            "nb_directors": x.nb_directors,
            "rank": x.loc["rank"],
        },
        axis=1,
    )

    # write
    logging.info("WRITE SERIALIZED DOCUMENTS")
    with open(join(DATA_DIR, OUTPUT_FILE_NAME), "w") as f:
        for s in serialized:
            f.write(simplejson.dumps(s, ignore_nan=True, cls=NpEncoder) + "\n")
