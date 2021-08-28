from typing import Optional, Union, Any, Tuple

from pandagg.types import AggName
from pandagg.node._node import Node


Value = Union[float, str]
Key = Union[float, str]


class Bucket(Node):
    def __init__(
        self, value: Optional[Value], level: Optional[str], key: Optional[Key] = None
    ) -> None:
        super(Bucket, self).__init__(keyed=False)
        self.level: Optional[AggName] = level
        self.key: Optional[Key] = key
        self.value: Optional[Value] = value

    @property
    def attr_name(self) -> str:
        """
        Determine under which attribute name the bucket will be available in response tree.
        Dots are replaced by `_` characters so that they don't prevent from accessing as attribute.

        Resulting attribute unfit for python attribute name syntax is still possible and will be accessible through
        item access (dict like), see more in 'utils.Obj' for more details.
        """
        # only for root
        if self.level is None:
            return ""
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


class RootBucket(Bucket):
    def __init__(self) -> None:
        super(RootBucket, self).__init__(level=None, key=None, value=None)
        self.keyed: bool = False
