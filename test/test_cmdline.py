import os
import sys
import time
import logging
import multiprocessing

import pytest

from clitest import CmdlineInterfaceTest

sys.path.insert(0, os.path.abspath('..'))


# If we want to measure code coverage across CLI invocations then we can do
# "coverage -x goeffel". Just need to figure out where to let coverage to
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


def test_pid(clitest, testprocess):
    clitest.run(f"goeffel --pid {testprocess.pid} -t 1")


def test_pid_command_simple(clitest, testprocess):
    clitest.run(f"goeffel --pid-command 'echo {testprocess.pid}' -t 1")


def test_hdf5_path_prefix_default(clitest, testprocess):
    clitest.run(f"goeffel --pid {testprocess.pid} -i 0.3 -t 1")
    clitest.expect_filename_pattern(
        r'^goeffel-timeseries__[0-9]+-[0-9]+\.hdf5$')


def test_hdf5_path_with_label(clitest, testprocess):
    clitest.run(
        f"goeffel --pid {testprocess.pid} -i 0.3 -t 1 "
        "--label custom-label"
    )
    clitest.expect_filename_pattern(
        r'^goeffel-timeseries_custom-label_[0-9]+-[0-9]+\.hdf5$')


def test_hdf5_path_prefix_custom(clitest, testprocess):
    clitest.run(
        f"goeffel --pid {testprocess.pid} -i 0.3 -t 1 "
        "--outfile-hdf5-path-prefix custom_prefix"
    )
    clitest.expect_filename_pattern(
        r'^custom_prefix__[0-9]+-[0-9]+\.hdf5$')


def test_hdf5_path_prefix_custom_and_label(clitest, testprocess):
    clitest.run(
        f"goeffel --pid {testprocess.pid} -i 0.3 -t 1 "
        "--outfile-hdf5-path-prefix custom_prefix --label custom-label"
    )
    clitest.expect_filename_pattern(
        r'^custom_prefix_custom-label_[0-9]+-[0-9]+\.hdf5$')


def test_hdf5_opt_collision(clitest, testprocess):
    clitest.run(
        f"goeffel --pid {testprocess.pid} -i 0.3 -t 1 "
        "--outfile-hdf5-path-prefix a --outfile-hdf5-path b",
        expect_rc=2
    )
    clitest.assert_in_stderr('not allowed with argument')


def test_hdf5_path(clitest, testprocess):
    clitest.run(
        f"goeffel --pid {testprocess.pid} -i 0.3 -t 1 "
        "--outfile-hdf5-path out.hdf5"
    )
    clitest.expect_filename_pattern(
        r'^out\.hdf5$')


def test_a_number_of_features_together(clitest, testprocess):
    clitest.run(
        f"goeffel --pid-command 'echo {testprocess.pid}' "
        "--diskstats sda --sampling-interval 0.3 -t 1"
    )


def test_analysis_inspect(clitest, testprocess):
    clitest.run(
        f"goeffel --pid {testprocess.pid} -i 0.3 -t 1 --outfile-hdf5-path out.hdf5"
    )
    clitest.run(
        f"goeffel-analysis inspect out.hdf5"
    )
    clitest.assert_in_stdout([
        'Created with: Goeffel',
        'Table properties:',
        'Column names:'
    ])


def test_analysis_plot(clitest, testprocess):
    clitest.run(
        f"goeffel --pid {testprocess.pid} -i 0.3 -t 1 --outfile-hdf5-path out.hdf5"
    )
    clitest.run(
        f"goeffel-analysis plot out.hdf5"
    )
    clitest.assert_in_stderr(['Writing figure as PNG to'])


def test_analysis_plot_additional_metric(clitest, testprocess):
    clitest.run(
        f"goeffel --pid {testprocess.pid} -i 0.3 -t 1 --outfile-hdf5-path out.hdf5"
    )
    clitest.run(
        f"goeffel-analysis plot out.hdf5 --metric system_loadavg1"
    )
    clitest.assert_in_stderr(['Writing figure as PNG to'])


def test_analysis_flexplot(clitest, testprocess):
    clitest.run(
        f"goeffel --pid {testprocess.pid} -i 0.3 -t 1 --outfile-hdf5-path out.hdf5"
    )
    clitest.run(
        "goeffel-analysis flexplot --series out.hdf5 'label' "
        "--column proc_num_ip_sockets_open 'ylabel' 'plottitle' 5"
    )
    clitest.assert_in_stderr(['Writing figure as PNG to'])
