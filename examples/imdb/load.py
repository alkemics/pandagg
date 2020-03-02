
import json
from os.path import join
from elasticsearch import Elasticsearch, helpers
from examples.imdb.conf import ES_HOST, DATA_DIR

index_name = 'movies'
mapping = {
    'properties': {
        'movie_id': {'type': 'integer'},
        'name': {
            'type': 'text',
            'fields': {
                'raw': {'type': 'keyword'}
            }
        },
        'year': {
            'type': 'date',
            'format': 'yyyy'
        },
        'rank': {'type': 'float'},
        # array
        'genres': {'type': 'keyword'},
        # nested
        'roles': {
            'type': 'nested',
            'properties': {
                'role': {'type': 'keyword'},
                'actor_id': {'type': 'integer'},
                'gender': {'type': 'keyword'},
                'first_name':  {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'last_name':  {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'full_name':  {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                }
            }
        },
        # nested
        'directors': {
            'type': 'nested',
            'properties': {
                'director_id': {'type': 'integer'},
                'first_name':  {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'last_name':  {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'full_name':  {
                    'type': 'text',
                    'fields': {
                        'raw': {'type': 'keyword'}
                    }
                },
                'genres': {
                    'type': 'keyword'
                }
            }
        },
        'nb_directors': {'type': 'integer'},
        'nb_roles': {'type': 'integer'}
    }
}


def bulk_index(client, docs):
    helpers.bulk(client=client, actions=[
        {
            '_index': index_name,
            '_op_type': 'index',
            '_id': document['movie_id'],
            '_source': document
        } for document in docs
    ])


if __name__ == '__main__':
    es_client = Elasticsearch(hosts=[ES_HOST])

    if not es_client.indices.exists(index=index_name):
        print('-' * 50)
        print('CREATE INDEX\n')
        es_client.indices.create(index_name)
    print('-' * 50)
    print('UPDATE MAPPING\n')
    es_client.indices.put_mapping(index=index_name, body=mapping)

    print('-' * 50)
    print('WRITE DOCUMENTS\n')
    docs_buffer = []
    with open(join(DATA_DIR, 'serialized.json'), 'r') as f:
        for l in f.readlines():
            if len(docs_buffer) >= 100:
                bulk_index(es_client, docs_buffer)
                docs_buffer = []
            s = json.loads(l)
            docs_buffer.append(s)
    if docs_buffer:
        bulk_index(es_client, docs_buffer)

    es_client.indices.refresh(index=index_name)
