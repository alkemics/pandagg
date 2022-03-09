import argparse
import logging
import os
from typing import List

import json
from typing import Iterator
from os.path import join
from elasticsearch import Elasticsearch

from examples.imdb.model import Movies
from pandagg.index import Action
import simplejson
import pandas as pd
import numpy as np

from os import path, remove
import csv
import mariadb


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


def download_table_as_csv(
    data_dir: str, overwrite: bool, cursor, table_name: str, column_names: List[str]
):
    cursor.execute("SELECT * FROM %s" % table_name)

    file_path = join(data_dir, "%s.csv" % table_name)
    if path.exists(file_path):
        if overwrite:
            remove(file_path)
        else:
            return

    with open(file_path, "w") as f:
        csv_dump = csv.writer(f)
        csv_dump.writerow(column_names)
        csv_dump.writerows(cursor.fetchall())


def download_tables(data_dir: str, overwrite: bool) -> None:

    # Using credentials defined here https://relational.fit.cvut.cz/dataset/IMDb
    conn = mariadb.connect(
        user="guest",
        password="relational",
        host="relational.fit.cvut.cz",
        port=3306,
        database="imdb_ijs",
    )
    cursor = conn.cursor()

    download_table_as_csv(
        data_dir=data_dir,
        overwrite=overwrite,
        cursor=cursor,
        table_name="directors_genres",
        column_names=["director_id", "genre", "prob"],
    )
    download_table_as_csv(
        data_dir=data_dir,
        overwrite=overwrite,
        cursor=cursor,
        table_name="directors",
        column_names=["id", "first_name", "last_name"],
    )
    download_table_as_csv(
        data_dir=data_dir,
        overwrite=overwrite,
        cursor=cursor,
        table_name="movies_directors",
        column_names=["director_id", "movie_id"],
    )
    download_table_as_csv(
        data_dir=data_dir,
        overwrite=overwrite,
        cursor=cursor,
        table_name="movies_genres",
        column_names=["movie_id", "genre"],
    )
    download_table_as_csv(
        data_dir=data_dir,
        overwrite=overwrite,
        cursor=cursor,
        table_name="movies",
        column_names=["id", "name", "year", "rank"],
    )
    download_table_as_csv(
        data_dir=data_dir,
        overwrite=overwrite,
        cursor=cursor,
        table_name="roles",
        column_names=["actor_id", "movie_id", "role"],
    )
    download_table_as_csv(
        data_dir=data_dir,
        overwrite=overwrite,
        cursor=cursor,
        table_name="actors",
        column_names=["id", "first_name", "last_name", "gender"],
    )


def serialize_documents(data_dir: str, limit: int) -> None:
    # tables imports
    reader_kwargs = {
        "encoding": "utf-8",
        "sep": ",",
        "quotechar": '"',
        "escapechar": "\\",
    }
    movies = pd.read_csv(
        join(data_dir, "movies.csv"), index_col="id", **reader_kwargs
    ).sample(n=limit)
    movies_genres = pd.read_csv(join(data_dir, "movies_genres.csv"), **reader_kwargs)
    movies_directors = pd.read_csv(
        join(data_dir, "movies_directors.csv"), **reader_kwargs
    )
    movies_directors = movies_directors[
        movies_directors.movie_id.isin(movies.index.values)
    ]
    directors = pd.read_csv(
        join(data_dir, "directors.csv"), index_col="id", **reader_kwargs
    )
    director_genres = pd.read_csv(
        join(data_dir, "directors_genres.csv"), **reader_kwargs
    )
    roles = pd.read_csv(join(data_dir, "roles.csv"), **reader_kwargs)
    roles = roles[roles.movie_id.isin(movies.index.values)]
    actors = pd.read_csv(join(data_dir, "actors.csv"), index_col="id", **reader_kwargs)

    # actors
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
    movie_serialized_genres = movies_genres.groupby("movie_id").genre.apply(list)

    # merge
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
    with open(join(data_dir, "index_documents.json"), "w") as f:
        for s in serialized:
            f.write(simplejson.dumps(s, ignore_nan=True, cls=NpEncoder) + "\n")


def operations_iterator(data_dir) -> Iterator[Action]:
    with open(join(data_dir, "index_documents.json"), "r") as f:
        for line in f.readlines():
            d = json.loads(line)
            yield {"_source": d, "_id": d["movie_id"]}


def setup_index_and_index_documents(data_dir: str, reset_index: bool) -> None:
    movies = Movies(client)
    if reset_index and movies.exists():
        movies.delete()
    movies.save()
    movies.docs.bulk(
        actions=operations_iterator(data_dir), _op_type_overwrite="index"
    ).perform()
    movies.refresh()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download and ingest IMDB data in an Elasticsearch cluster.\nBy default the used cluster will be "
        "localhost:9200, overwrite client options in this file to customize this behavior."
    )
    parser.add_argument(
        "--dir",
        type=str,
        default="temp-data",
        help="path to directory where the downloaded files will be stored (default 'temp-data')",
    )
    parser.add_argument(
        "--overwrite",
        action="store_const",
        const=True,
        default=False,
        help="if True, re-download files even if they are already present (default False)",
    )
    parser.add_argument(
        "--resetindex",
        action="store_const",
        const=True,
        default=False,
        help="if True, index is deleted and recreated even though it already exists (default False)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20000,
        help="number of movies that will be ingested (default 20000)",
    )
    args = parser.parse_args()

    client = Elasticsearch(
        # specify your config here
    )

    if not path.exists(args.dir):
        os.mkdir(args.dir)

    logging.info("Download tables from remote sql database")
    download_tables(data_dir=args.dir, overwrite=args.overwrite)
    logging.info("Serialize documents")
    serialize_documents(data_dir=args.dir, limit=args.limit)
    logging.info("Index documents in cluster")
    setup_index_and_index_documents(data_dir=args.dir, reset_index=args.resetindex)
