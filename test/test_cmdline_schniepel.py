import os
import sys
import logging

import pytest

from clitest import CmdlineInterfaceTest

sys.path.insert(0, os.path.abspath('..'))


# If we want to measure code coverage across CLI invocations then we can do
# "coverage -x schniepel". Just need to figure out where to let coverage to
# store its state.


logging.basicConfig(
    format='%(asctime)s,%(msecs)-6.1f %(funcName)s# %(message)s',
    datefmt='%H:%M:%S')
log = logging.getLogger()
log.setLevel(logging.DEBUG)


@pytest.fixture
def clitest(tmp_path, request):
    testname = request.node.name
    c = CmdlineInterfaceTest(
        name=testname,
        rundir=str(tmp_path),
        preamble_lines=['export PYTHONIOENCODING="utf-8"']
    )
    yield c


def test_a_number_of_features(clitest):
    clitest.run(
        "schniepel --pid-command 'pgrep gnome-shell | head -n1' "
        "--diskstats sda --sampling-interval 0.3 --terminate-after-n-samples 1"
    )
