# adapted from elasticsearch-dsl-py

import dataclasses
from copy import deepcopy
from typing import Optional, Any, List, Dict, Tuple, Union
from typing_extensions import TypedDict, Literal

from elasticsearch import Elasticsearch, helpers

from pandagg import Mappings, Search
from pandagg.tree.mappings import MappingsDictOrNode
from pandagg.types import SettingsDict, IndexAliases, DocSource, IndexName


class Action(TypedDict, total=False):
    _op_type: Literal["create", "index", "update", "delete"]
    _id: str
    _index: IndexName
    retry_on_conflict: int
    routing: str
    version: int
    version_type: Literal["external", "external_gte"]
    _source: DocSource
    doc: DocSource
    require_alias: bool
    dynamic_templates: Dict


@dataclasses.dataclass
class DocumentBulkWriter:
    _index: "DeclarativeIndex"
    _operations: List[Action] = dataclasses.field(init=False, default_factory=list)

    @property
    def _client(self) -> Elasticsearch:
        return self._index._get_connection()

    def bulk(self, actions: List[Action]) -> "DocumentBulkWriter":
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
        # https://elasticsearch-py.readthedocs.io/en/master/helpers.html
        self._operations.extend(actions)
        return self

    def index(
        self,
        _source: DocSource,
        op_type: Literal["create", "index"] = "index",
        _id: Optional[str] = None,
        **kwargs: Any
    ) -> "DocumentBulkWriter":
        # if create: fails if _id already present
        # '_id' will be generated automatically if not present
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html
        # upsert (create or update)
        self._operations.append(
            {
                "_id": _id,
                "_source": _source,
                "_index": self._index.name,
                "_op_type": op_type,
                **kwargs,  # type: ignore
            }
        )
        return self

    def update(self, _id: str, doc: DocSource, **kwargs: Any) -> "DocumentBulkWriter":
        """
        Update an existing document.

        Note that update of inner object is partial:

        >>> index.index(_id="john", doc={"personal_info": {"surname": "John", "lastname": "Doe"}})
        >>> index.update(_id="john", doc={"personal_info": {"surname": "Bob"}})

        would result in following source:
        {"personal_info": {"surname": "Bob", "lastname": "Doe"}}

        Ie: "personal_info.lastname" isn't modified, inner documents are not fully replaced, but only present keys are.

        Whereas, providing elements in an array replace source key as a whole:

        >>> index.index(_id="john", doc={"personal_info": {"surname": "John", "lastname": "Doe"}})
        >>> index.update(_id="john", doc={"personal_info": [{"surname": "Bob"}]})

        will result in replaceing whole "personal_info" field:

        {"personal_info": [{"surname": "Bob"}]}
        """
        self._operations.append(
            {
                "_id": _id,
                "doc": doc,
                "_index": self._index.name,
                "_op_type": "update",
                **kwargs,  # type: ignore
            }
        )
        return self

    def delete(self, _id: str, **kwargs: Any) -> "DocumentBulkWriter":
        self._operations.append(
            {
                "_id": _id,
                "_index": self._index.name,
                "_op_type": "delete",
                **kwargs,  # type: ignore
            }
        )
        return self

    def perform(self, **kwargs: Any) -> Tuple[int, Union[int, List[Any]]]:
        # return success, failed
        # perform stacked operations
        res = helpers.bulk(
            client=self._client, actions=list(self._operations), **kwargs
        )
        self._operations = []
        return res

    def rollback(self) -> None:
        # remove all stacked operations
        self._operations = []


class IndexMeta(type):

    # global flag to apply changes to descendants of DeclarativeIndex class (and not the abstract class itself)
    _abstract_index_initialized = False

    def __new__(cls, name: str, bases: Tuple, attrs: Dict) -> "IndexMeta":
        if not cls._abstract_index_initialized:
            # only happens for DeclarativeIndex abstract class
            cls._abstract_index_initialized = True
            return super(IndexMeta, cls).__new__(cls, name, bases, attrs)

        # Ensure name is defined, and copy mappings / settings / aliases to prevent any unintended mutation.
        if not attrs.get("name"):
            raise ValueError("<%s> declarative index must have a name" % name)
        if attrs.get("mappings") is not None:
            attrs["mappings"] = deepcopy(attrs["mappings"])
        attrs["_mappings"] = Mappings(**attrs["mappings"])
        if attrs.get("settings") is not None:
            attrs["settings"] = deepcopy(attrs["settings"])
        if attrs.get("aliases") is not None:
            attrs["aliases"] = deepcopy(attrs["aliases"])
        return super(IndexMeta, cls).__new__(cls, name, bases, attrs)


class DeclarativeIndex(metaclass=IndexMeta):

    name: str
    mappings: Optional[MappingsDictOrNode] = None
    settings: Optional[SettingsDict] = None
    aliases: Optional[IndexAliases] = None

    # initialized by metaclass, from mappings declaration
    _mappings: Mappings

    def __init__(self, client: Optional[Elasticsearch] = None) -> None:
        self._client: Optional[Elasticsearch] = client
        self.docs: DocumentBulkWriter = DocumentBulkWriter(_index=self)

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        if self.settings:
            out["settings"] = self.settings
        if self.aliases:
            out["aliases"] = self.aliases
        if self.mappings is not None:
            mappings = self._mappings.to_dict() or {}
            if mappings:
                out["mappings"] = mappings
        return out

    def _get_connection(self) -> Elasticsearch:
        if self._client is None:
            raise ValueError(
                "An Elasticsearch client must be provided in order to execute queries."
            )
        return self._client

    def search(
        self, nested_autocorrect: bool = False, repr_auto_execute: bool = False
    ) -> Search:
        return Search(
            using=self._client,
            mappings=self._mappings,
            index=self.name,
            nested_autocorrect=nested_autocorrect,
            repr_auto_execute=repr_auto_execute,
        )

    def create(self, **kwargs: Any) -> Any:
        """
        Creates the index in elasticsearch.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.create`` unchanged.
        """
        return self._get_connection().indices.create(
            index=self.name, body=self.to_dict(), **kwargs
        )

    def is_closed(self) -> bool:
        state = self._get_connection().cluster.state(index=self.name, metric="metadata")
        return state["metadata"]["indices"][self.name]["state"] == "close"

    def exists(self, **kwargs: Any) -> bool:
        """
        Returns ``True`` if the index already exists in elasticsearch.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.exists`` unchanged.
        """
        return self._get_connection().indices.exists(index=self.name, **kwargs)

    def get_settings(self, **kwargs: Any) -> Dict:
        """
        Retrieve settings for the index.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.get_settings`` unchanged.
        """
        return self._get_connection().indices.get_settings(index=self.name, **kwargs)

    def _put_settings(self, **kwargs: Any) -> Any:
        """
        As a private method in DeclarativeIndex, because only supposed to be used while persisting declared index
        settings.

        Change specific index level settings in real time.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.put_settings`` unchanged.
        """
        return self._get_connection().indices.put_settings(index=self.name, **kwargs)

    def _put_mappings(self, **kwargs: Any) -> Any:
        """
        As a private method in DeclarativeIndex, because only supposed to be used while persisting declared index
        mappings.

        Register specific mapping definition for a specific type.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.put_mapping`` unchanged.
        """
        return self._get_connection().indices.put_mapping(index=self.name, **kwargs)

    def save(self) -> None:
        """
        Sync the index definition with elasticsearch, creating the index if it
        doesn't exist and updating its settings and mappings if it does.

        Note some settings and mapping changes cannot be done on an open
        index (or at all on an existing index) and for those this method will
        fail with the underlying exception.
        """
        if not self.exists():
            return self.create()

        body = self.to_dict()
        settings = body.pop("settings", {})
        current_settings = self.get_settings()[self.name]["settings"]["index"]

        # try and update the settings
        if settings:
            settings = settings.copy()
            for k, v in list(settings.items()):
                if k in current_settings and current_settings[k] == str(v):
                    del settings[k]
            if settings:
                self._put_settings(body=settings)

        # update the mappings, any conflict in the mappings will result in an
        # exception
        mappings = body.pop("mappings", {})
        if mappings:
            self._put_mappings(body=mappings)

    def analyze(self, **kwargs: Any) -> Any:
        """
        Perform the analysis process on a text and return the tokens breakdown
        of the text.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.analyze`` unchanged.
        """
        return self._get_connection().indices.analyze(index=self.name, **kwargs)

    def refresh(self, **kwargs: Any) -> Any:
        """
        Performs a refresh operation on the index.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.refresh`` unchanged.
        """
        return self._get_connection().indices.refresh(index=self.name, **kwargs)

    def flush(self, **kwargs: Any) -> Any:
        """
        Performs a flush operation on the index.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.flush`` unchanged.
        """
        return self._get_connection().indices.flush(index=self.name, **kwargs)

    def get(self, **kwargs: Any) -> Any:
        """
        The get index API allows to retrieve information about the index.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.get`` unchanged.
        """
        return self._get_connection().indices.get(index=self.name, **kwargs)

    def open(self, **kwargs: Any) -> Any:
        """
        Opens the index in elasticsearch.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.open`` unchanged.
        """
        return self._get_connection().indices.open(index=self.name, **kwargs)

    def close(self, **kwargs: Any) -> Any:
        """
        Closes the index in elasticsearch.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.close`` unchanged.
        """
        return self._get_connection().indices.close(index=self.name, **kwargs)

    def delete(self, **kwargs: Any) -> Any:
        """
        Deletes the index in elasticsearch.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.delete`` unchanged.
        """
        return self._get_connection().indices.delete(index=self.name, **kwargs)

    def stats(self, **kwargs: Any) -> Any:
        """
        Retrieve statistics on different operations happening on the index.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.stats`` unchanged.
        """
        return self._get_connection().indices.stats(index=self.name, **kwargs)

    def segments(self, **kwargs: Any) -> Any:
        """
        Provide low level segments information that a Lucene index (shard
        level) is built with.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.segments`` unchanged.
        """
        return self._get_connection().indices.segments(index=self.name, **kwargs)

    def validate_query(self, **kwargs: Any) -> Any:
        """
        Validate a potentially expensive query without executing it.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.validate_query`` unchanged.
        """
        return self._get_connection().indices.validate_query(index=self.name, **kwargs)

    def clear_cache(self, **kwargs: Any) -> Any:
        """
        Clear all caches or specific cached associated with the index.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.clear_cache`` unchanged.
        """
        return self._get_connection().indices.clear_cache(index=self.name, **kwargs)

    def recovery(self, **kwargs: Any) -> Any:
        """
        The indices recovery API provides insight into on-going shard
        recoveries for the index.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.recovery`` unchanged.
        """
        return self._get_connection().indices.recovery(index=self.name, **kwargs)

    def flush_synced(self, **kwargs: Any) -> Any:
        """
        Perform a normal flush, then add a generated unique marker (sync_id) to
        all shards.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.flush_synced`` unchanged.
        """
        return self._get_connection().indices.flush_synced(index=self.name, **kwargs)

    def shard_stores(self, **kwargs: Any) -> Any:
        """
        Provides store information for shard copies of the index. Store
        information reports on which nodes shard copies exist, the shard copy
        version, indicating how recent they are, and any exceptions encountered
        while opening the shard index or from earlier engine failure.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.shard_stores`` unchanged.
        """
        return self._get_connection().indices.shard_stores(index=self.name, **kwargs)

    def forcemerge(self, **kwargs: Any) -> Any:
        """
        The force merge API allows to force merging of the index through an
        API. The merge relates to the number of segments a Lucene index holds
        within each shard. The force merge operation allows to reduce the
        number of segments by merging them.

        This call will block until the merge is complete. If the http
        connection is lost, the request will continue in the background, and
        any new requests will block until the previous force merge is complete.

        Any additional keyword arguments will be passed to
        ``Elasticsearch.indices.forcemerge`` unchanged.
        """
        return self._get_connection().indices.forcemerge(index=self.name, **kwargs)