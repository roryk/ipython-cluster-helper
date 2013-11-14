#!/usr/bin/env python
from setuptools import setup, find_packages

setup(name = "ipython-cluster-helper",
      version = "0.2.2",
      author = "Rory Kirchner",
      author_email = "rory.kirchner@gmail.com",
      description = "Simplify IPython cluster start up and use for multiple schedulers.",
      license = "MIT",
      zip_safe=False,
      url = "https://github.com/roryk/ipython-cluster-helper",
      namespace_packages = ["cluster_helper"],
      packages = find_packages(),
      install_requires = [
          "pyzmq >= 2.1.11",
          "ipython >= 1.1.0"])
