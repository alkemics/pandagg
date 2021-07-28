from lighttree import AutoIdNode as OriginalNode

from pandagg.utils import DSLMixin
from typing import Dict, Any


class Node(DSLMixin, OriginalNode):

    NID_SIZE: int = 8

    @staticmethod
    def expand__to_dot(params: Dict[str, Any]) -> Dict[str, Any]:
        nparams: Dict[str, Any] = {}
        for pname, pvalue in params.items():
            if "__" in pname:
                pname = pname.replace("__", ".")
            nparams[pname] = pvalue
        return nparams
