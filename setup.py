#!/usr/bin/env python
from setuptools import setup, find_packages
from pip.req import parse_requirements
from pip.download import PipSession

install_reqs = parse_requirements("requirements.txt", session=PipSession())
reqs = [str(ir.req) for ir in install_reqs]

setup(name = "ipython-cluster-helper",
      version = "0.5.2",
      author = "Rory Kirchner",
      author_email = "rory.kirchner@gmail.com",
      description = "Simplify IPython cluster start up and use for multiple schedulers.",
      long_description=(open('README.rst').read()),
      license = "MIT",
      zip_safe=False,
      url = "https://github.com/roryk/ipython-cluster-helper",
      packages = find_packages(),
      install_requires = reqs)
