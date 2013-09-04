#!/usr/bin/env python
from setuptools import setup, find_packages

setup(name = "ipython-cluster-helper",
      version = "0.1.13",
      author = "Rory Kirchner",
      author_email = "rory.kirchner@gmail.com",
      description = "Simplify IPython cluster start up and use for multiple schedulers.",
      license = "MIT",
      url = "https://github.com/roryk/ipython-cluster-helper",
      namespace_packages = ["cluster_helper"],
      dependency_links = ['http://github.com/ipython/ipython/tarball/master#egg=ipython-1.0.dev'],
      packages = find_packages(),
      install_requires = [
          "pyzmq >= 2.1.11",
          "ipython >= 0.13.2"])
