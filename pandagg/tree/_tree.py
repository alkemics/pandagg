from lighttree import Tree as OriginalTree


class Tree(OriginalTree):
    def __str__(self) -> str:
        return "<{class_}>\n{tree}".format(
            class_=str(self.__class__.__name__), tree=self.show(limit=40)
        )

    def __repr__(self) -> str:
        return self.__str__()
