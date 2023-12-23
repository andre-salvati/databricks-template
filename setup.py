"""
This file configures the Python package with entrypoints used for future runs on Databricks.

Please follow the `entry_points` documentation for more details on how to configure the entrypoint:
* https://setuptools.pypa.io/en/latest/userguide/entry_point.html
"""

from setuptools import find_packages, setup
from tasks import __version__

setup(
    name="template",
    packages=find_packages(exclude=["tests", "tests.*"]),
    setup_requires=["wheel"],
    version=__version__,
    description="",
    author="",
    install_requires=[
        'funcy==2.0',
    ]
)
