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
]

install_requires = [
    'treelib==1.5.5',
    'elasticsearch==2.3.0'
]

extras_require = {
    'test': tests_require
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
    zip_safe= False,
    install_requires=install_requires,
    tests_require=tests_require,
    extras_require=extras_require,
)
