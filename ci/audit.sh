#!/bin/bash
set -ex

python setup.py check
#python setup.py --long-description | rst2html.py > /dev/null
#rst2html.py CHANGELOG.rst > /dev/null

# Run flake8 on all the Python files it discovers.
flake8

# The pylint result is not to be interpreted in a binary fashion.
# pylint --reports=n --disable=C0103,W0212,W0511,W0142,R0903 gipc/gipc.py

# Build documentation.
#cd docs && make html

# See if this would be good to release.
python setup.py sdist
twine check dist/*