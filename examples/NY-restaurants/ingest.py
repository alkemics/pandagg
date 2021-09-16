# adapted from https://github.com/elastic/elasticsearch-py/blob/main/examples/bulk-ingest/bulk-ingest.py

"""Script that downloads a public dataset and streams it to an Elasticsearch cluster"""

import csv
from os.path import abspath, join, dirname, exists
import urllib3
from elasticsearch import Elasticsearch

from pandagg.index import DeclarativeIndex

NYC_RESTAURANTS = (
    "https://data.cityofnewyork.us/api/views/43nn-pn8j/rows.csv?accessType=DOWNLOAD"
)
DATASET_PATH = join(dirname(abspath(__file__)), "nyc-restaurants.csv")
CHUNK_SIZE = 16384


class NYCRestaurants(DeclarativeIndex):
    name = "nyc-restaurants"
    mappings = {
        "properties": {
            "name": {"type": "text"},
            "borough": {"type": "keyword"},
            "cuisine": {"type": "keyword"},
            "grade": {"type": "keyword"},
            "score": {"type": "integer"},
            "location": {"type": "geo_point"},
            "inspection_date": {"type": "date", "format": "MM/dd/yyyy"},
        }
    }
    settings = {"number_of_shards": 1}


def download_dataset():
    """Downloads the public dataset if not locally downloaded
    and returns the number of rows are in the .csv file.
    """
    if not exists(DATASET_PATH):
        http = urllib3.PoolManager()
        resp = http.request("GET", NYC_RESTAURANTS, preload_content=False)

        if resp.status != 200:
            raise RuntimeError("Could not download dataset")

        with open(DATASET_PATH, mode="wb") as f:
            chunk = resp.read(CHUNK_SIZE)
            while chunk:
                f.write(chunk)
                chunk = resp.read(CHUNK_SIZE)

    with open(DATASET_PATH) as f:
        return sum([1 for _ in f]) - 1


def generate_actions():
    """Reads the file through csv.DictReader() and for each row
    yields a single document. This function is passed into the bulk()
    helper to create many documents in sequence.
    """
    with open(DATASET_PATH, mode="r") as f:
        reader = csv.DictReader(f)

        for row in reader:
            source = {
                "name": row["DBA"],
                "borough": row["BORO"],
                "cuisine": row["CUISINE DESCRIPTION"],
                "grade": row["GRADE"] or None,
                "score": row["SCORE"] or None,
                "inspection_date": row["INSPECTION DATE"] or None,
            }
            lat = row["Latitude"]
            lon = row["Longitude"]
            if lat not in ("", "0") and lon not in ("", "0"):
                source["location"] = {"lat": float(lat), "lon": float(lon)}
            yield {"_id": row["CAMIS"], "_source": source, "_op_type": "index"}


if __name__ == "__main__":
    print("Loading dataset...")
    number_of_docs = download_dataset()

    client = Elasticsearch(
        # Add your cluster configuration here!
    )
    index = NYCRestaurants(client)

    if index.exists():
        print("Removing previous index")
        index.delete()

    print("Creating an index...")
    index.save()

    print("Indexing documents...")
    index.docs.bulk(generate_actions()).perform()

    print("Indexed %d documents" % number_of_docs)
