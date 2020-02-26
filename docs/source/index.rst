.. pandagg documentation master file, created by
   sphinx-quickstart on Sat Feb  1 22:30:48 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

pandagg
=======

.. toctree::
   :hidden:
   :maxdepth: 4

   user-guide
   advanced-usage
   API reference <reference/pandagg>
   contributing

pandagg is a Python package providing a simple interface to manipulate ElasticSearch queries and aggregations. It brings
the following features:

- flexible aggregation and search queries declaration
- query validation based on provided mapping
- parsing of aggregation results in handy format: interactive bucket tree, normalized tree or tabular breakdown
- mapping interactive navigation

`IMDB example <_external/imdb_exploration.html>`_

pandagg is easy to use::

    >>> from pandagg.agg import Agg, Term, DateRange
    >>> a = Agg()\
    >>>     .groupby(DateRange('creationYear', field=creation))\
    >>>     .agg({'avg'})


Installing
----------

pandagg can be installed with `pip <https://pip.pypa.io>`_::

    $ pip install pandagg

Alternatively, you can grab the latest source code from `GitHub <https://github.com/alkemics/pandagg>`_::

    $ git clone git://github.com/alkemics/pandagg.git
    $ python setup.py install

Usage
-----

The :doc:`user-guide` is the place to go to learn how to use the library and
accomplish common tasks. The more in-depth :doc:`advanced-usage` guide is the place to go for deeply nested queries.

The :doc:`reference/pandagg` documentation provides API-level documentation.

License
-------

pandagg is made available under the MIT License. For more details, see `LICENSE.txt <https://github.com/alkemics/pandagg/blob/master/LICENCE>`_.

Contributing
------------

We happily welcome contributions, please see :doc:`contributing` for details.
