"""
setup.py configuration script describing how to build and package this project.

This file is primarily used by the setuptools library and typically should not
be executed directly. See README.md for how to deploy, test, and run
the default_python project.
"""

from setuptools import setup, find_packages

import datetime
import sys

sys.path.append("./src")

import template

setup(
    name="template",
    # We use timestamp as Local version identifier (https://peps.python.org/pep-0440/#local-version-identifiers.)
    # to ensure that changes to wheel package are picked up when used on all-purpose clusters
    version=template.__version__ + "+" + datetime.datetime.utcnow().strftime("%Y%m%d.%H%M%S"),
    author="user@company.com",
    description="wheel file based on template/src",
    packages=find_packages(where="./src"),
    package_dir={"": "src"},
    include_package_data=True,
    package_data={
        "template": ["*.ini"],
    },
    entry_points={"packages": ["main=template.main:main"]},
    install_requires=[
        # Dependencies in case the output wheel file is used as a library dependency.
        # For defining dependencies, when this package is used in Databricks, see:
        # https://docs.databricks.com/dev-tools/bundles/library-dependencies.html
        "setuptools",
        "funcy",
        "databricks-sdk",
    ],
)
