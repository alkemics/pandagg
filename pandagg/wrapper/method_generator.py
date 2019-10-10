#!/usr/bin/env python
# -*- coding: utf-8 -*-

import inspect


def make_signature(func):
    """(copied from pandas)
    Returns a tuple containing the parameters list with defaults
    and parameter list.

    Examples
    --------
    >>> def f(a, b, c=2):
    >>>     return a * b * c
    >>> print(make_signature(f))
    (['a', 'b', 'c=2'], ['a', 'b', 'c'])
    """

    spec = inspect.getargspec(func)
    if spec.defaults is None:
        n_wo_defaults = len(spec.args)
        defaults = ('',) * n_wo_defaults
    else:
        n_wo_defaults = len(spec.args) - len(spec.defaults)
        defaults = ('',) * n_wo_defaults + tuple(spec.defaults)
    args = []
    for var, default in zip(spec.args, defaults):
        args.append(var if default == '' else var + '=' + repr(default))
    if spec.varargs:
        args.append('*' + spec.varargs)
    if spec.keywords:
        args.append('**' + spec.keywords)
    return args, spec.args


def _method_generator(klass):
    """
    Parameters
    ----------
    klass : class
        class where members are defined. Should be an AggregationNode

    :return: string, suitable for exec'ing, that define implementations of the named method for Indice class.
    -------
    """

    wrapper_template = \
        """def %(name)s(%(sig)s) :
    \"""
    %(name)s aggregation:
    %(doc)s
    \"""
    return self.aggregate(%(klass_name)s(%(args)s))
    """
    # global is a restricted keyword
    final_name = klass.AGG_TYPE if klass.AGG_TYPE != 'global' else 'global_agg'
    f = getattr(klass, '__init__')
    doc = f.__doc__
    doc = doc if type(doc) == str else ''
    decl, args = make_signature(f)
    args_by_name = ['{0}={0}'.format(arg) for arg in args[1:]]
    params = {
        'name': final_name,
        'doc': doc,
        'sig': ','.join(decl),
        'self': args[0],
        'args': ','.join(args_by_name),
        'klass_name': klass.__name__
    }
    return wrapper_template % params
