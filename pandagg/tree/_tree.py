from lighttree import Tree as OriginalTree

from pandagg.utils import DSLMixin


class Tree(DSLMixin, OriginalTree):

    KEY: str
    _type_name: str

    def __str__(self) -> str:
        return "<{class_}>\n{tree}".format(
            class_=str(self.__class__.__name__), tree=self.show(limit=40)
        )

    def __repr__(self) -> str:
        return self.__str__()
