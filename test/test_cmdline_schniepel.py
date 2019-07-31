import os
import sys
import time
import logging
import multiprocessing

import pytest

from clitest import CmdlineInterfaceTest

sys.path.insert(0, os.path.abspath('..'))


# If we want to measure code coverage across CLI invocations then we can do
# "coverage -x schniepel". Just need to figure out where to let coverage to
# store its state.


logging.basicConfig(
    format='%(asctime)s,%(msecs)-6.1f %(name)s %(funcName)s# %(message)s',
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


@pytest.fixture(scope='session', autouse=True)
def testprocess():

    def _run():
        while True:
            time.sleep(0.1)

    log.info('Start test process')
    p = multiprocessing.Process(target=_run)
    # Make it less likely for the test runner to leave behind an orphaned child.
    p.daemon = True
    try:
        p.start()
        yield p
    finally:
        p.terminate()
        p.join()
        log.info('Test process terminated cleanly')


def test_a_number_of_features(clitest, testprocess):
    clitest.run(
        f"schniepel --pid-command 'echo {testprocess.pid}' "
        "--diskstats sda --sampling-interval 0.3 --terminate-after-n-samples 1"
    )
