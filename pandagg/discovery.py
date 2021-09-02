from dataclasses import dataclass
from typing import Dict, Any, Optional

from lighttree.interactive import Obj
from elasticsearch import Elasticsearch

from pandagg import Mappings, MappingsDict
from pandagg.interactive.mappings import IMappings
from pandagg.search import Search


@dataclass
class Index:
    name: str
    settings: Dict[str, Any]
    mappings: MappingsDict
    aliases: Any
    client: Optional[Elasticsearch] = None

    @property
    def imappings(self) -> IMappings:
        # TODO- create mypy issue
        mappings: Mappings = Mappings(**self.mappings)  # type: ignore
        return IMappings(mappings=mappings, client=self.client, index=[self.name])

    def search(
        self, nested_autocorrect: bool = True, repr_auto_execute: bool = True
    ) -> Search:
        return Search(
            using=self.client,
            mappings=self.mappings,
            index=self.name,
            nested_autocorrect=nested_autocorrect,
            repr_auto_execute=repr_auto_execute,
        )

    def __str__(self) -> str:
        return self.__repr__()

    def __repr__(self) -> str:
        return "<Index '%s'>" % self.name


class Indices(Obj):
    _COERCE_ATTR = True


def discover(using: Elasticsearch, index: str = "*") -> Indices:
    """
    :param using: Elasticsearch client
    :param index: Comma-separated list or wildcard expression of index names used to limit the request.
    """
    indices = Indices()
    for index_name, index_detail in using.indices.get(index=index).items():
        indices[index_name] = Index(
            client=using,
            name=index_name,
            mappings=index_detail["mappings"],
            settings=index_detail["settings"],
            aliases=index_detail["aliases"],
        )
    return indices
