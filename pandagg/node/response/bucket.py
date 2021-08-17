from typing import Optional, Union, Any, Tuple

from pandagg.node._node import Node


Value = Union[float, str]
Key = Union[float, str]


class BucketNode(Node):
    def __init__(self) -> None:
        # level holds aggregation name
        self.level: Optional[str] = None
        super(BucketNode, self).__init__(keyed=False)


class Bucket(BucketNode):
    def __init__(self, value: Value, level: str, key: Optional[Key] = None) -> None:
        super(Bucket, self).__init__()
        self.level: str = level
        self.key: Optional[Key] = key
        self.value: Value = value

    @property
    def attr_name(self) -> str:
        """
        Determine under which attribute name the bucket will be available in response tree.
        Dots are replaced by `_` characters so that they don't prevent from accessing as attribute.

        Resulting attribute unfit for python attribute name syntax is still possible and will be accessible through
        item access (dict like), see more in 'utils.Obj' for more details.
        """
        if self.key is not None:
            return "%s_%s" % (self.level.replace(".", "_"), self._coerced_key)
        return self.level.replace(".", "_")

    def line_repr(self, depth: int, **kwargs: Any) -> Tuple[str, str]:
        s = self.level or ""
        if self.key is not None:
            s += "=%s" % self._coerced_key
        return s, str(self.value) if self.value else ""

    @property
    def _coerced_key(self) -> Optional[Key]:
        key = self.key
        try:
            # order matters, will
            key = float(key)  # type: ignore
            key = int(key)
        except (ValueError, TypeError):
            pass
        return key
