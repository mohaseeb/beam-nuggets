from __future__ import division, print_function

from setuptools import setup, find_packages

REQUIRED_TEST_PACKAGES = ['nose']
setup(
    name='beam-nuggets',
    version='0.3.0.dev1',
    install_requires=[
        'apache-beam>=2.8.0,<3.0.0',
        'SQLAlchemy>=1.2.14,<2.0.0',
        'psycopg2-binary>=2.7.6.1,<3.0.0',
        'sqlalchemy-utils>=0.33.8,<0.34'
    ],
    extras_require={'dev': REQUIRED_TEST_PACKAGES},
    packages=find_packages(exclude=("test", "tests"))
)
