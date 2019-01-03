from __future__ import division, print_function

from setuptools import setup, find_packages

REQUIRED_TEST_PACKAGES = [
    'nose>=1.3.7,<2.0.0',
    'testing.postgresql=>1.3.0,<2.0.0',
    'numpy>=1.15.4,<2.0.0',
    'pandas>=0.23.4,<0.24'
]

setup(
    name='beam-nuggets',
    version='0.6.0.dev1',
    install_requires=[
        'apache-beam>=2.8.0,<3.0.0',
        'SQLAlchemy>=1.2.14,<2.0.0',
        'psycopg2-binary>=2.7.6.1,<3.0.0',
        # FIXME dialects for different DBs?
        'sqlalchemy-utils>=0.33.8,<0.34'
    ],
    extras_require={'dev': REQUIRED_TEST_PACKAGES},
    packages=find_packages(exclude=("test", "tests"))
)
