from .field_datatypes import FIELD_DATATYPES
from .meta_fields import META_FIELDS


FIELDS = {f.KEY: f for f in FIELD_DATATYPES + META_FIELDS}


def deserialize_field(name, body, depth=0, is_subfield=False):
    type_ = body.get('type', 'object')
    if type_ not in FIELDS:
        raise NotImplementedError('Unknown field type <%s>' % type_)
    klass = FIELDS.get(type_)
    return klass.deserialize(name=name, body=body, depth=depth, is_subfield=is_subfield)
