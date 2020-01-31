from ._leaf_clause import deserialize_leaf_clause
from ._compound import deserialize_compound_clause
from ._parameter_clause import deserialize_parameter


def deserialize_node(k, body, accept_leaf=True, accept_compound=True, accept_param=True):
    if not (accept_leaf or accept_compound or accept_param):
        raise ValueError('Must accept at least one type of deserialization.')

    if accept_leaf:
        try:
            return deserialize_leaf_clause(k, body)
        except Exception as e_l:
            pass
    if accept_compound:
        try:
            return deserialize_compound_clause(k, body)
        except Exception as e_c:
            pass
    if accept_param:
        try:
            return deserialize_parameter(k, body)
        except Exception as e_p:
            pass
    if accept_compound:
        raise e_c
    if accept_leaf:
        raise e_l
    if accept_param:
        raise e_p
