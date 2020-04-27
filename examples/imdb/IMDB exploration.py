#!/usr/bin/env python
# coding: utf-8

# # IMDB exploration with Pandagg

# This tutorial will guide you in some of pandagg functionalities, exploring IMDB data.
#
# 1. Cluster indices discovery
# 2. Mapping exploration
# 3. Aggregations
# 4. Queries
#

# In[1]:


# requires to be declared on top
import matplotlib.pyplot as plt
import seaborn


# ## 1. Cluster indices discovery

# In[2]:


# instanciate client just as you would do with regular elastic client
from pandagg import Elasticsearch

client = Elasticsearch(hosts=["localhost:9300"])


# In[3]:


# indices instance lists available indices of cluster, with their mappings and settings
indices = client.fetch_indices()
indices


# Indices are accessible with autocompletion
#
# ![autocomplete](ressources/autocomplete_index.png)

# In[4]:


movies = indices.movies
movies


# ## 2. Mapping exploration

# In[5]:


# mapping is accessible via "mapping" attribute of "Index" instance
m = movies.mapping

# this is equivalent to instanciate Mapping from dict as follow:
from examples.imdb.load import mapping as imdb_mapping
from pandagg.mapping import IMapping

m = IMapping(imdb_mapping, client=client, index_name="movies")

# Note: client and index_name arguments are optional, but doing so provides the ability to
# compute "quick-access" aggregations on fields (will be detailed below)


# In[6]:


# you can navigate (with help of tab autocomplete) into your mapping
m.directors


# In[7]:


# going deeper
m.directors.full_name


# In[8]:


# calling instance will display mapping definition
m.directors.full_name()


# ### Quick access aggregations from mapping

# Mapping leaves (here genres) all have a "a" attribute (for aggregation).
# Autocomplete will display all possible aggregations types on this field
#
# ![autocomplete](ressources/autocomplete_agg.png)
#

# In[9]:


# passed parameters will be added to aggregation body
m.genres.a.terms(missing="N/A", size=5)


# In[10]:


m.rank.a.stats()


# In[11]:


# query parameter enable to filter on some conditions
# documentaries are overall better ranked than average of movies
m.rank.a.stats(query={"term": {"genres": "Documentary"}})


# ## 3. Aggregations

# Let's compute the number of movies per decade per genre.
#

# ### Regular declaration

# In[12]:


regular_syntax = {
    "genres": {
        "terms": {"field": "genres", "size": 3},
        "aggs": {
            "movie_decade": {
                "date_histogram": {"field": "year", "fixed_interval": "3650d"}
            }
        },
    }
}

from pandagg.aggs import Aggs

agg = Aggs(regular_syntax)

assert agg.to_dict() == regular_syntax

agg


# ### DSL syntax
# The following syntaxes are strictly equivalent to the above one:

# In[13]:


from pandagg.aggs import DateHistogram, Terms

agg_dsl = Aggs(
    Terms(
        "genres",
        field="genres",
        size=3,
        aggs=DateHistogram(name="movie_decade", field="year", fixed_interval="3650d"),
    )
)

# or using groupby method: the listed aggregations will be placed from top to bottom:

agg_variant = Aggs().groupby(
    [
        Terms("genres", field="genres", size=3),
        DateHistogram("movie_decade", field="year", fixed_interval="3650d"),
    ]
)


assert agg_dsl.to_dict() == agg_variant.to_dict()
assert agg_dsl.to_dict() == regular_syntax

# decade = DateHistogram('movie_decade', field='year', fixed_interval='3650d')
# per_decate_genres = movies.groupby(['genres', decade],size=3).execute()
agg_dsl


# #### About groupby and agg methods
#
# - `groupby` method will arrange passed aggregations clauses "vertically" (nested manner),
# - `agg` method will arrange them "horizontally"

# In[14]:


Aggs().groupby(
    [
        Terms("genres", field="genres", size=3),
        DateHistogram("movie_decade", field="year", fixed_interval="3650d"),
    ]
)


# In[15]:


Aggs().aggs(
    [
        Terms("genres", field="genres", size=3),
        DateHistogram("movie_decade", field="year", fixed_interval="3650d"),
    ]
)


# Both `groupby` and `agg` will place provided aggregations under the `insert_below` (parent id) aggregation clause if `insert_below` is provided, else under the deepest bucket aggregation if there is no ambiguity:
# ```
# OK: A──> B ─> C ─> NEW_AGGS
#
# KO: A──> B
#     └──> C
# ```

# In[16]:


# taking again this example
example_agg = Aggs(regular_syntax)
example_agg


# In[17]:


# groupby behaviour
example_agg.groupby(["roles.role", "roles.gender"], insert_below="genres")


# In[18]:


# agg behaviour
example_agg.aggs(["roles.role", "roles.gender"], insert_below="genres")


# ### Aggregation execution and parsing

# Aggregation instance can be bound to an Elasticsearch client, either at `__init__`, either using `bind` method.

# In[19]:


agg_dsl.bind(client=client, index_name="movies")


# Doing so provides the ability to execute aggregation request, and parse the response in multiple formats. Formats will be detailed in next example, here we use the dataframe format:
#
# *Note: requires to install **pandas** dependency*

# In[20]:


per_decate_genres = agg_dsl.execute(output="dataframe")
per_decate_genres.unstack()


# In[21]:


per_decate_genres.unstack().T.plot(figsize=(12, 12))


# **Another example:**
# who are the actors who have played in the highest number of movies between 1990 and 2000, and what was the average ranking of the movies they played in per genre?
#

# In[22]:


from datetime import datetime
from pandagg.aggs import Aggs, Avg, Min, Max
from pandagg.query import Range


# in groupby and agg methods,
agg = (
    Aggs(client=client, index_name="movies", mapping=imdb_mapping)
    .groupby(["roles.full_name.raw", "genres"], size=2)
    .aggs(
        [
            Avg("avg_rank", field="rank"),
            Min("min_date", field="year"),
            Max("max_date", field="year"),
        ]
    )
    .query(Range(field="year", gte="2000", lt="2010"))
)

print(agg)
r = agg.execute()

r["min_year"] = r.min_date.apply(lambda x: datetime.fromtimestamp(x / 1000.0).year)
r["max_year"] = r.max_date.apply(lambda x: datetime.fromtimestamp(x / 1000.0).year)
r


# #### As raw output

# In[23]:


# agg.execute(output='raw')


# #### As interactive tree

# In[24]:


t = agg.execute(output="tree")
t


# #### Navigation with autocompletion

# In[25]:


t.roles_full_name_raw_Grey_DeLisle__599599_


# #### List documents in given bucket (with autocompletion)

# In[26]:


delisle_adventure = t.roles_full_name_raw_Grey_DeLisle__599599_.reverse_nested_below_roles_full_name_raw.genres_Adventure.list_documents(
    _source=["id", "genres", "name"], size=2
)


# In[27]:


delisle_adventure


# ## 4. Queries

# Suppose I want:
# - actions or thriller movies
# - with ranking >= 7
# - with a female actor playing a reporter role

# ### Regular syntax

# We would perform the following request:

# In[28]:


expected_query = {
    "bool": {
        "must": [
            {"terms": {"genres": ["Action", "Thriller"]}},
            {"range": {"rank": {"gte": 7}}},
            {
                "nested": {
                    "path": "roles",
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {"roles.gender": {"value": "F"}}},
                                {"term": {"roles.role": {"value": "Reporter"}}},
                            ]
                        }
                    },
                }
            },
        ]
    }
}


# We can build our Query instance using this regular syntax:

# In[29]:


from pandagg.query import Query

q = Query(expected_query)
q


# ### DSL syntax

# With pandagg DSL syntax, it could also be declared this way:

# In[30]:


from pandagg.query import Nested, Bool, Query, Range, Term, Terms as TermsFilter

# warning, pandagg.query.Terms and pandagg.agg.Terms classes have same name, but one is a filter, the other an aggreggation

q = Query(
    Bool(
        must=[
            TermsFilter("genres", terms=["Action", "Thriller"]),
            Range("rank", gte=7),
            Nested(
                path="roles",
                query=Bool(
                    must=[
                        Term("roles.gender", value="F"),
                        Term("roles.role", value="Reporter"),
                    ]
                ),
            ),
        ]
    )
)


# In[31]:


# query computation
q.to_dict() == expected_query


# Suppose you want to expose a route to your customers with actionable filters, it is easy to add query clauses at specific places in your query by chaining your clauses:
#

# In[32]:


# accepts mix of DSL and dict syntax

my_query = (
    Query()
    .must(TermsFilter("genres", terms=["Action", "Thriller"]))
    .must({"range": {"rank": {"gte": 7}}})
    .must(
        Nested(
            path="roles",
            query=Bool(
                must=[
                    {"term": {"roles.gender": {"value": "F"}}},
                    {"term": {"roles.role": {"value": "Reporter"}}},
                ]
            ),
        )
    )
)

my_query.to_dict() == expected_query


# ### Advanced query declaration using _named queries_
#
# We can take advantage of [named queries](https://www.elastic.co/guide/en/elasticsearch/reference/6.8/search-request-named-queries-and-filters.html) to specifically declare where we want to insert a clause.
#
# A simple use case could be to expose some filters to a client among which some apply to nested clauses (for instance nested 'roles').

# In[33]:


from pandagg.utils import equal_queries

# suppose API exposes those filters
genres_in = ["Action", "Thriller"]
rank_above = 7
filter_role_gender = "F"
filter_role = "Reporter"


q = Query()


if genres_in is not None:
    q = q.must(TermsFilter("genres", terms=genres_in))
if rank_above is not None:
    q = q.must(Range("rank", gte=rank_above))

# we name the nested query that we would potentially use
q = q.query(Nested(_name="nested_roles", path="roles"))
# a compound clause (bool, nested etc..) without any children clauses is not serialized
assert q.to_dict() == {
    "bool": {
        "must": [
            {"terms": {"genres": ["Action", "Thriller"]}},
            {"range": {"rank": {"gte": 7}}},
        ]
    }
}


# we declare that those clauses must be placed below 'nested_roles' condition
if filter_role_gender is not None:
    q = q.query(Term("roles.gender", value=filter_role_gender), parent="nested_roles")
if filter_role is not None:
    q = q.query(Term("roles.role", value=filter_role), parent="nested_roles")

assert equal_queries(q.to_dict(), expected_query)
q


# In[ ]:
