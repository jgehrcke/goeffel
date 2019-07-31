#!/bin/bash
set -x

# Install newer pip and setuptools (newer than bundles with certain Python
# releases and newer than what Travis brings) -- but still pin the versions so
# that there are no moving dependencies.
pip install 'pip==19.2.1' --upgrade
pip install 'setuptools==41.0.1' --upgrade

# Install Schniepel dependencies from its `setup.py`.
pip install .

# Install test/CI dependencies.
pip install -r requirements-tests.txt

