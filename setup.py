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
    'coverage',
    'nose',
    'mock',
    'flake8',
    'pandas>=0.24.2',
]

install_requires = [
    'future',
    'treelib>=1.6.1,<2.0.0',
    'elasticsearch>=7.1.0,<8.0.0',
    'six>=1.13.0,<2.0.0'
]

extras_require = {
    'test': tests_require,
    'pandas': ['pandas>=0.24.2']
}

# =============================================================================
#                                      SETUP
# =============================================================================

setup(
    name='pandagg',
    version='0.0.1',
    description='Python package provided to make elasticsearch aggregation easy, inspired by pandas library.',
    long_description=README,
    long_description_content_type='text/markdown',
    classifiers=[],
    author='Léonard Binet',
    author_email='leonardbinet@gmail.com',
    url='https://github.com/alkemics/pandagg',
    download_url='https://github.com/alkemics/pandagg/archive/v0.0.1.tar.gz',
    keywords='elasticsearch aggregation pandas',
    packages=find_packages(),
    include_package_data=True,
    test_suite='pandagg.tests',
    zip_safe=False,
    install_requires=install_requires,
    tests_require=tests_require,
    extras_require=extras_require,
)
