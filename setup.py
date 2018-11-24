from __future__ import division, print_function

from setuptools import setup, find_packages

setup(
    name='beam-nuggets',
    version='0.1.0.dev1',
    install_requires=[
        'apache-beam',
    ],
    packages=find_packages()
)
