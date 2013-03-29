#!/usr/bin/env python
from setuptools import setup, find_packages

setup(name = "ipython-cluster-helper",
      version = "0.1.2",
      author = "Rory Kirchner",
      author_email = "rory.kirchner@gmail.com",
      description = "Simplify IPython cluster start up and use for multiple schedulers.",
      license = "MIT",
      url = "https://github.com/roryk/ipython-cluster-helper",
      namespace_packages = ["cluster_helper"],
      packages = find_packages(),
      install_requires = [
          "pyzmq == 2.2.0.1",
          "ipython >= 0.13.1"])
