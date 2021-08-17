from textwrap import dedent
from typing import Any, Callable


# Substitution and Appender are copied from pandas.util._decorators
# https://github.com/pandas-dev/pandas/blob/master/LICENSE


class Substitution:
    """
    A decorator to take a function's docstring and perform string
    substitution on it.

    This decorator should be robust even if func.__doc__ is None
    (for example, if -OO was passed to the interpreter)

    Usage: construct a docstring.Substitution with a sequence or
    dictionary suitable for performing substitution; then
    decorate a suitable function with the constructed object. e.g.

    sub_author_name = Substitution(author='Jason')

    @sub_author_name
    def some_function(x):
        "%(author)s wrote this function"

    # note that some_function.__doc__ is now "Jason wrote this function"

    One can also use positional arguments.

    sub_first_last_names = Substitution('Edgar Allen', 'Poe')

    @sub_first_last_names
    def some_function(x):
        "%s %s wrote the Raven"
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if args and kwargs:
            raise AssertionError("Only positional or keyword args are allowed")

        self.params = args or kwargs

    def __call__(self, func: Callable) -> Callable:
        func.__doc__ = func.__doc__ and func.__doc__ % self.params
        return func

    def update(self, *args: Any, **kwargs: Any) -> None:
        """
        Update self.params with supplied args.
        """

        if isinstance(self.params, dict):
            self.params.update(*args, **kwargs)


class Appender:
    """
    A function decorator that will append an addendum to the docstring
    of the target function.

    This decorator should be robust even if func.__doc__ is None
    (for example, if -OO was passed to the interpreter).

    Usage: construct a docstring.Appender with a string to be joined to
    the original docstring. An optional 'join' parameter may be supplied
    which will be used to join the docstring and addendum. e.g.

    add_copyright = Appender("Copyright (c) 2009", join='\n')

    @add_copyright
    def my_dog(has='fleas'):
        "This docstring will have a copyright below"
        pass
    """

    def __init__(self, addendum: str, join: str = "", indents: int = 0) -> None:
        if indents > 0:
            self.addendum = indent(addendum, indents=indents)
        else:
            self.addendum = addendum
        self.join = join

    def __call__(self, func: Callable) -> Callable:
        func.__doc__ = func.__doc__ if func.__doc__ else ""
        self.addendum = self.addendum if self.addendum else ""
        docitems = [func.__doc__, self.addendum]
        func.__doc__ = dedent(self.join.join(docitems))
        return func


def indent(text: str, indents: int = 1) -> str:
    jointext = "".join(["\n"] + ["    "] * indents)
    return jointext.join(text.split("\n"))
