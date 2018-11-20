#!/usr/bin/env python

from setuptools import setup

requirements = [

    'pytest',
    'sphinxcontrib-bibtex'
]

try:
    from multiply_forward_operators import __version__ as version
except ImportError:
    version = 'unknown'

setup(name='dummy_repository',
      version=__version__,
      description='Dummy Repository',
      author='...',
      packages=['dummy_repository'],
      install_requires=requirements
      )
