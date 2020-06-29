#!/usr/bin/env python
# -*- coding: utf-8 -*-

__version__ = "0.1.2"

import os

from setuptools import setup
from setuptools import find_packages


here = os.path.abspath(os.path.dirname(__file__))
README = open(os.path.join(here, "README.md")).read()

install_requires = ["six", "future", "lighttree==0.0.8", "elasticsearch>=7.0.0,<8.0.0"]


setup(
    name="pandagg",
    version=__version__,
    description="Python package provided to make elasticsearch aggregations and queries easy.",
    long_description=README,
    long_description_content_type="text/markdown",
    author="Léonard Binet",
    author_email="leonardbinet@gmail.com",
    url="https://github.com/alkemics/pandagg",
    keywords="elasticsearch aggregation pandas",
    packages=find_packages(),
    include_package_data=True,
    test_suite="pandagg.tests",
    zip_safe=False,
    install_requires=install_requires,
    setup_requires=["pytest-runner"],
    license="MIT",
)
