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
    'pandas==0.24.2'
]

install_requires = [
    'future',
    'treelib==1.5.6',
    'elasticsearch==7.1.0',
    'six==1.13.0'
]

extras_require = {
    'test': tests_require,
    'dev': ['pandas==0.24.2']
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
    # until treelib v1.6.0 release: https://github.com/caesar0301/treelib/issues/128
    dependency_links=['git+https://github.com/leonardbinet/treelib@node_dict_pointer#egg=treelib-1.5.6']
)
