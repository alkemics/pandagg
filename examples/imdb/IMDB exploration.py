#!/usr/bin/env python
# coding: utf-8

# # Pandagg overview

# In[1]:


import seaborn
import matplotlib.pyplot as plt
from pandagg import Elasticsearch


# ## Explore cluster indices

# In[2]:


client = Elasticsearch(hosts=['localhost:9300'])


# In[3]:


# indices instance lists available indices of cluster, with their mappings and settings
indices = client.fetch_indices()
indices


# Indices are accessible with autocompletion
# 
# ![autocomplete](ressources/autocomplete_index.png)

# In[4]:


movies = indices.movies


# ## Mapping exploration

# In[5]:


# mapping is accessible via "mapping" attribute of "Index" instance
m = movies.mapping

# this is equivalent to instanciate Mapping from dict as follow:
from examples.imdb.load import mapping as imdb_mapping
from pandagg.mapping import IMapping

m2 = IMapping(imdb_mapping, client=client)


# In[6]:


# you can navigate (with help of tab autocomplete) into your mapping
m.directors


# In[7]:


# calling instance will display mapping definition
m.directors.last_name()


# ## Quick access aggregations from mapping

# Mapping leaves (here genres) all have a "a" attribute (for aggregation). 
# Autocomplete will display all possible aggregations types on this field
# 
# ![autocomplete](ressources/autocomplete_agg.png)
# 

# In[8]:


# passed parameters will be added to aggregation body
m.genres.a.terms(missing='N/A', size=5)


# In[9]:


m.rank.a.stats()


# In[10]:


# query parameter enable to filter on some conditions
# documentaries are overall better ranked than average of movies
m.rank.a.stats(query={'term': {'genres': 'Documentary'}})


# In[11]:


from pandagg.agg import DateHistogram

decade = DateHistogram('movie_decade', field='year', fixed_interval='3650d')
per_decate_genres = movies.groupby(['genres', decade],size=3).execute()


# In[12]:


per_decate_genres.unstack()


# In[13]:


per_decate_genres.unstack().T.plot(figsize=(12,12))


# In[14]:


from datetime import datetime
from pandagg.agg import Avg
from pandagg.query import Range

agg = movies    .groupby(['roles.full_name.raw', 'genres'], size=3)    .agg([Avg('avg_rank', field='rank'), Avg('avg_date', field='year')])    .query(Range(field='year', gte='1990', lt='2000'))

r = agg.execute()
        
r['avg_year'] = r.avg_date.apply(lambda x: datetime.fromtimestamp(x / 1000.).year)
r


# ## Aggregation result navigation

# ### As raw output

# In[15]:


# agg.execute(output='raw')


# ### As interactive tree

# In[16]:


t = agg.execute(output='tree')
t


# #### Navigation with autocompletion

# In[17]:


t.roles_full_name_raw_Frank_Welker__506067_


# #### List documents in given bucket (with autocompletion)

# In[18]:


frank_welker_family = t    .roles_full_name_raw_Frank_Welker__506067_    .reverse_nested_below_roles_full_name_raw.genres_Family.list_documents(compact=False)


# In[19]:


frank_welker_family.keys()


# ## Query declaration

# In[20]:


def ordered(obj):
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj

def equal_queries(d1, d2):
    """Compares if two queries are equivalent (do not consider nested list orders).
    Note: this is greedy.
    """
    return ordered(d1) == ordered(d2)


# Suppose I want: 
# - actions or thriller movies
# - with ranking >= 7
# - with a female actor playing a reporter role
# 
# I would perform the following request:

# In[21]:


expected_query = {'bool': {'must': [
    {'terms': {'genres': ['Action', 'Thriller']}},
    {'range': {'rank': {'gte': 7}}},
    {'nested': {
        'path': 'roles',
        'query': {'bool': {'must': [
            {'term': {'roles.gender': {'value': 'F'}}},
            {'term': {'roles.role': {'value': 'Reporter'}}}]}
         }
    }}
]}}


# We can use regular syntax.

# In[22]:


from pandagg.query import Query

q = Query(expected_query)
q


# With pandagg DSL syntax, it could also be declared this way:

# In[23]:


from pandagg.query import Nested, Bool, Query, Terms, Range, Term

q = Query(
    Bool(must=[
        Terms('genres', terms=['Action', 'Thriller']),
        Range('rank', gte=7),
        Nested(
            path='roles', 
            query=Bool(must=[
                Term('roles.gender', value='F'),
                Term('roles.role', value='Reporter')
            ])
        )
    ])
)


# In[24]:


# query computation
q.query_dict() == expected_query


# Suppose you want to expose a route to your customers with actionable filters, it is easy to add query clauses at specific places in your query by chaining your clauses:
# 

# In[25]:


# accepts mix of DSL and dict syntax

my_query = Query()    .must(Terms('genres', terms=['Action', 'Thriller']))    .must({'range': {'rank': {'gte': 7}}})    .must(
        Nested(
            path='roles', 
            query=Bool(must=[
                {'term': {'roles.gender': {'value': 'F'}}}, 
                {'term': {'roles.role': {'value': 'Reporter'}}}
            ]
      )
    )
)

my_query.query_dict() == expected_query


# ### Advanced query declaration using _named queries_
# 
# We can take advantage of [named queries](https://www.elastic.co/guide/en/elasticsearch/reference/6.8/search-request-named-queries-and-filters.html) to specifically declare where we want to insert a clause.
# 
# A simple use case could be to expose some filters to a client among which some apply to nested clauses (for instance nested 'roles').

# In[26]:


# suppose API exposes those filters
genres_in = ['Action', 'Thriller']
rank_above = 7
filter_role_gender = 'F'
filter_role = 'Reporter'


q = Query()


if genres_in is not None:
    q = q.must(Terms('genres', terms=genres_in))
if rank_above is not None:
    q = q.must(Range('rank', gte=rank_above))

# we name the nested query that we would potentially use
q = q.query(Nested(_name='nested_roles', path='roles'))
# a compound clause (bool, nested etc..) without any children clauses is not serialized
assert q.query_dict() == {'bool': {'must': [
    {'terms': {'genres': ['Action', 'Thriller']}},
    {'range': {'rank': {'gte': 7}}}
]}}


# we declare that those clauses must be placed below 'nested_roles' condition
if filter_role_gender is not None:
    q = q.query(Term('roles.gender', value=filter_role_gender), parent='nested_roles')
if filter_role is not None:
    q = q.query(Term('roles.role', value=filter_role), parent='nested_roles')

assert equal_queries(q.query_dict(), expected_query)
q


# In[ ]:




