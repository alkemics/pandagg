class TreeReprMixin:
    # mixin rather than inheritance so that mypy generics can be specified in Tree subclasses
    def __str__(self) -> str:
        return "<{class_}>\n{tree}".format(
            class_=str(self.__class__.__name__),
            tree=self.show(limit=40),  # type: ignore
        )

    def __repr__(self) -> str:
        return self.__str__()
