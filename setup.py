#!/usr/bin/env python
import subprocess
import sys
from setuptools import setup, find_packages
from io import open

def parse_requirements(fn):
    """ load requirements from a pip requirements file """
    return [line for line in open(fn) if line and not line.startswith("#")]

reqs = parse_requirements("requirements.txt")

setup(name="ipython-cluster-helper",
      version="0.6.3",
      author="Rory Kirchner",
      author_email="rory.kirchner@gmail.com",
      description="Simplify IPython cluster start up and use for "
      "multiple schedulers.",
      long_description=(open('README.rst', encoding='utf-8').read()),
      license="MIT",
      zip_safe=False,
      url="https://github.com/roryk/ipython-cluster-helper",
      packages=find_packages(),
      install_requires=reqs)
