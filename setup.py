from __future__ import division, print_function

from setuptools import setup, find_packages

REQUIRED_TEST_PACKAGES = [
    'nose>=1.3.7,<2.0.0',
    'testing.postgresql>=1.3.0,<2.0.0',
    'testing.mysqld>=1.4.0,<2.0.0',
    'numpy>=1.15.4,<2.0.0',
    'pandas>=0.23.4,<0.24'
]

REQUIRED_DOCUMENTATION = [
    'Sphinx>=1.8.3,<2.0.0',
    'sphinx_rtd_theme>=0.4.2,<2.0.0'
]

setup(
    name='beam-nuggets',
    version='0.7.0',
    install_requires=[
        'apache-beam>=2.8.0,<3.0.0',
        'SQLAlchemy>=1.2.14,<2.0.0',
        'sqlalchemy-utils>=0.33.8,<0.34',
        # Below is for connection to specific DBs
        'psycopg2-binary>=2.7.6.1,<3.0.0',
        'PyMySQL>=0.9.3,<2.0.0'
        # FIXME what about the other DBs supported by sqlalchemy
    ],
    extras_require={'dev': REQUIRED_TEST_PACKAGES + REQUIRED_DOCUMENTATION},
    packages=find_packages(exclude=("test", "tests"))
)
