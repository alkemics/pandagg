#!/usr/bin/env python
# coding: utf-8

"""Module to extract
"""

from os.path import join
import simplejson
import pandas as pd
from .conf import DATA_DIR, OUTPUT_FILE_NAME


if __name__ == '__main__':

    print('-' * 50)
    print('LOADING TABLES\n')
    # tables imports
    reader_kwargs = {
        "encoding": 'utf-8',
        "sep": ',',
        "quotechar": '"',
        "escapechar": '\\'
    }
    movies = pd.read_csv(
        join(DATA_DIR, 'movies.csv'),
        index_col='id',
        **reader_kwargs
    )
    movies_genres = pd.read_csv(
        join(DATA_DIR, 'movies_genres.csv'),
        **reader_kwargs
    )
    movies_directors = pd.read_csv(
        join(DATA_DIR, 'movies_directors.csv'),
        **reader_kwargs
    )
    directors = pd.read_csv(
        join(DATA_DIR, 'directors.csv'),
        index_col='id',
        **reader_kwargs
    )
    director_genres = pd.read_csv(
        join(DATA_DIR, 'directors_genres.csv'),
        **reader_kwargs
    )
    roles = pd.read_csv(
        join(DATA_DIR, 'roles.csv'),
        **reader_kwargs
    )
    actors = pd.read_csv(
        join(DATA_DIR, 'actors.csv'),
        index_col='id',
        **reader_kwargs
    )

    # actors
    print('-' * 50)
    print('SERIALIZE ACTORS\n')
    actor_roles = pd.merge(actors, roles, left_index=True, right_on='actor_id')
    actor_roles['serialized'] = actor_roles.apply(lambda x: {
        'actor_id': x.actor_id,
        'first_name': x.first_name,
        'last_name': x.last_name,
        'full_name': '%s %s (%s)' % (x.first_name, x.last_name, x.actor_id),
        'gender': x.gender,
        'role': x.role
    }, axis=1)
    movie_serialized_actors = actor_roles.groupby('movie_id').serialized.apply(list)

    # directors
    print('-' * 50)
    print('SERIALIZE DIRECTORS\n')
    directors_grouped_genres = pd.DataFrame(director_genres.groupby('director_id').genre.apply(list))
    movie_directors_extended = pd.merge(movies_directors, directors, left_on='director_id', right_index=True)
    movie_directors_extended = pd.merge(movie_directors_extended, directors_grouped_genres, how='left', left_on='director_id', right_index=True)
    movie_directors_extended['serialized'] = movie_directors_extended.apply(lambda x: {
        'director_id': x.director_id,
        'first_name': x.first_name,
        'last_name': x.last_name,
        'full_name': '%s %s (%s)' % (x.first_name, x.last_name, x.director_id),
        'genres': x.genre,
    }, axis=1)
    movie_serialized_directors = pd.DataFrame(movie_directors_extended.groupby('movie_id').serialized.apply(list))

    # movie genres
    print('-' * 50)
    print('SERIALIZE MOVIE GENRES\n')
    movie_serialized_genres = movies_genres.groupby('movie_id').genre.apply(list)

    # merge
    print('-' * 50)
    print('MERGE DATASETS\n')
    enriched_movies = pd.merge(movies, movie_serialized_actors, how='left', left_index=True, right_index=True)
    enriched_movies = pd.merge(enriched_movies, movie_serialized_directors, how='left', left_index=True, right_index=True)
    enriched_movies = pd.merge(enriched_movies, movie_serialized_genres, how='left', left_index=True, right_index=True)

    serialized = enriched_movies.apply(lambda x: {
        'movie_id': x.name,
        'name': x.loc['name'],
        'year': x.year,
        'genres': x.genre,
        'roles': x.serialized_x,
        'directors': x.serialized_y,
        'rank': x.loc['rank']
    }, axis=1)

    # write
    print('-' * 50)
    print('WRITE SERIALIZED DOCUMENTS\n')
    with open(join(DATA_DIR, OUTPUT_FILE_NAME), 'w') as f:
        for s in serialized:
            f.write(simplejson.dumps(s, ignore_nan=True) + '\n')
