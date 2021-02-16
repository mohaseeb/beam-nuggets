from __future__ import division, print_function

from setuptools import setup, find_packages

VERSION = '0.17.1'

REQUIRED_PACKAGES = [
    'apache-beam>=2.8.0,<3.0.0',
    'SQLAlchemy>=1.2.14,<2.0.0',
    'sqlalchemy-utils>=0.33.11,<0.34',
    # Below are drivers for connection to specific DBs
    # 'pg8000>=1.12.4,<2.0.0',
    'pg8000<=1.16.5',
    # https://stackoverflow.com/questions/64519570/sqlalchemy-orm-call-fails-with-typeerror-expected-bytes-str-found
    'PyMySQL>=0.9.3,<2.0.0',
    'kafka-python>=2.0.1',
]

REQUIRED_PACKAGES_TEST = [
    'nose>=1.3.7,<2.0.0',
    'testing.postgresql>=1.3.0,<2.0.0',
    'testing.mysqld>=1.4.0,<2.0.0',
    'numpy>=1.15.4,<2.0.0',
    'pandas'
]

REQUIRED_PACKAGES_DOCS = [
    'Sphinx>=1.8.3,<2.0.0',
    'sphinx_rtd_theme>=0.4.2,<2.0.0'
]

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='beam-nuggets',
    version=VERSION,
    author='Mohamed Haseeb',
    author_email='m@mohaseeb.com',
    description='Collection of transforms for the Apache beam python SDK.',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/mohaseeb/beam-nuggets",
    install_requires=REQUIRED_PACKAGES,
    extras_require={'dev': REQUIRED_PACKAGES_TEST + REQUIRED_PACKAGES_DOCS},
    packages=find_packages(exclude=("test", "tests")),
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ]
)
