#!/usr/bin/env python

from setuptools import setup

requirements = [

    'pytest',
    'sphinxcontrib-bibtex'
]

__version__ = None
with open('sar_pre_processing/version.py') as f:
    exec(f.read())

setup(name='dummy_repository',
      version=__version__,
      description='Dummy Repository',
      author='...',
      packages=['dummy_repository'],
      install_requires=requirements
)
