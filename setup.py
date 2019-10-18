#!/usr/bin/env python
# -*- coding: utf-8 -*-

# =============================================================================
#                                      IMPORTS
# =============================================================================

import os

from setuptools import setup
from setuptools import find_packages


# =============================================================================
#                                    DEFINITIONS
# =============================================================================

here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, 'README.md')).read()

tests_require = [
    'nose',
    'mock',
    'flake8',
    'coverage',
    'elasticsearch==2.3.0',
    'pandas==0.24.2'
]

install_requires = [
    # treelib when https://github.com/caesar0301/treelib/pull/120 is approved
]

extras_require = {
    'test': tests_require,
    'dev': [
        'elasticsearch==2.3.0',
        'pandas==0.24.2'
    ]
}

# =============================================================================
#                                      SETUP
# =============================================================================

setup(
    name='pandagg',
    version='0.1',
    description='Python package provided to make elasticsearch aggregation easy, inspired by pandas library.',
    long_description=README,
    classifiers=[],
    author='Léonard Binet',
    author_email='leonardbinet@gmail.com',
    keywords='elasticsearch aggregation pandas',
    packages=find_packages(),
    include_package_data=True,
    test_suite='pandagg.tests',
    zip_safe=False,
    install_requires=install_requires,
    tests_require=tests_require,
    extras_require=extras_require,
    # waiting for https://github.com/caesar0301/treelib/pull/120 approval
    dependency_links=['http://github.com/leonardbinet/treelib/tarball/node_dict_pointer']
)
