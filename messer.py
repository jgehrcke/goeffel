#!/usr/bin/env python
#
# MIT License

# Copyright (c) 2018 Jan-Philip Gehrcke

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

"""
This program measures the resource utilization of a specific process at a
specific sampling rate. Some of the measurement values are the time average of a
metric measured over the time interval between two samples; other measurement
values are simply reflecting a momentary state in the moment of taking the
sample, and again other value types are accumulative (as in the number of events
that happened between two samples).

This program collects the time series data in a buffer and uses a separate
process for emitting these data for later inspection (that is, measurement is
decoupled from data emission making the sampling rate more predictable when
persisting data on disk).

In addition to sampling process-specific data, this program also measures
the utilization of system-wide resources making it straightforward to put the
process-specific metrics into context.

This program has been written for and tested on CPython 3.5 and 3.6 on Linux.
"""

import argparse
import textwrap
import logging
import sys
import time
import os
import subprocess
import multiprocessing

from datetime import datetime


"""
This was born out of a need for solid tooling. We started with pidstat from
sysstat, launched in the following manner:

 pidstat -hud -p $PID 1 1

We found that it does not properly account for multiple threads running in the
same process, and that various issues in that regard exist in this program
across various versions. Related references:

 - https://github.com/sysstat/sysstat/issues/73#issuecomment-349946051
 - https://github.com/sysstat/sysstat/commit/52977c479
 - https://github.com/sysstat/sysstat/commit/a63e87996

The program cpustat open-sourced by Uber
(https://github.com/uber-common/cpustat) has a promising README about the
general measurement methodology, and overall seems to be a great tool for our
ourposes but the methodology around persisting the collected timeseries data is
undocumented. It seems to write some binary file when using `-cpuprofile` but it
is a little unclear what this file contains.

The program psrecord (https://github.com/astrofrog/psrecord) which effectively
wraps psutil has the same fundamental idea as this code here; it however does
not have a clear separation of concerns in the code between persisting the data
to disk, performing the measurements themselves, and plotting the data,
rendering it not production-ready for our concerns.

References:

https://github.com/Leo-G/DevopsWiki/wiki/How-Linux-CPU-Usage-Time-and-Percentage-is-calculated
http://www.brendangregg.com/usemethod.html
https://github.com/uber-common/cpustat

"""


log = logging.getLogger()
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s.%(msecs)03d %(levelname)s: %(message)s",
    datefmt="%y%m%d-%H:%M:%S"
    )


# The sample interval should ideally be larger than 0.2 seconds so that kernel
# counter update errors and other timing errors do not dominate the data emitted
# by this program. Relevant reference: https://elinux.org/Kernel_Timer_Systems
# From the `cpustat` documentation: "Linux CPU time accounting is done in terms
# of whole "clock ticks", which are often 100ms. This can cause some strange
# values when sampling every 200ms.""


PROCESS_SAMPLE_INTERVAL_SECONDS = 1
PROCESS_PID_POLL_INTERVAL_SECONDS = 2.0


def main():

    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description='A tool for measuring system resource utilization',
        epilog=textwrap.dedent(__doc__).strip()
    )

    what = parser.add_mutually_exclusive_group(required=True)

    what.add_argument(
        '--pid-command',
        metavar='\'PID COMMAND\'',
    )
    what.add_argument('--pid', metavar='PROCESSID', type=int)

    # Collect ideas for more flexible configuration.
    # what.add_argument('--diskusage', action='store_true')
    # parser.add_argument('--outfile', metavar='PATH')

    args = parser.parse_args()

    # Set up the infrastructure for decoupling measurement (sample collection)
    # from persisting the data.
    samplequeue = multiprocessing.Queue()
    samplewriter = multiprocessing.Process(
        target=sample_writer_process,
        args=(samplequeue,)
    )
    samplewriter.start()

    try:
        mainloop(args, samplequeue)
    except KeyboardInterrupt:
        sys.exit(0)
    finally:
        # Signal to the worker that it is supposed to (cleanly) shut down. Wait
        # for the queue buffer to be flushed to the worker process, and wait for
        # the sample writer process to terminate.
        samplequeue.put(None)
        samplequeue.close()
        samplequeue.join_thread()
        samplewriter.join()
        log.debug('Sample writer process terminated')


def sample_writer_process(queue):
    """
    This is being run in a child process. The parent and the child are connected
    with a classical pipe for inter-process communication, on top of which an
    atomic message queue is implemented for passing messages from the parent
    process to the child process. Each message is either a sample object
    (collection of values for a given timestamp), or `None` to signal the child
    process that it is supposed to properly shut down and exit.

    Ignore SIGINT (default handler in CPython is to raise KeyboardInterrupt,
    which is undesired here). The parent handles it, and instructs the child to
    clean up as part of handling it.
    """
    import signal
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    while True:
        sample = queue.get()

        # `sample` is expected to be an n-tuple or None. Shut down worker
        # process in case of `None`.
        if sample is None:
            log.debug('Sample writer process: shutting down')
            break

        # Instead of writing to `stdout`, in the future, this is going to
        # support writing a (binary) file format directly to disk. For this to
        # not affect the main loop (as of disk write hiccups / latencies) this
        # being is in a separate process, so that the sampling (measurement)
        # itself is decoupled from persisting the data through a Queue-based
        # buffer (in the parent process) between both processes.
        print(
            '%s, %5.2f, %5.2f, %5.2f, '
            '%d, %d, %d, %d, %d, %d, %d, %d, %d, '
            '%d, %5.2f, %5.2f, %5.2f, %d, %d, %d, %d, %d, %d' % (sample))


def mainloop(args, samplequeue):
    """
    Wraps the main sampling loop. Is intended to be interruptible via SIGINT /
    Ctrl+C, and therefore wrapped in a `KeyboardInterrupt` exception handler
    in the caller.
    """

    # Import here, delayed, instead of in the header (otherwise the command line
    # help / error reporting is noticeably slowed down).
    import psutil

    # Handle case where a specific (constant) PID was provided on the command
    # line. Error out and exit program in the moment the PID cannot be monitored
    # (as of process not existing, insufficient permissions, etc.).
    if args.pid is not None:
        try:
            sample_process(args.pid, samplequeue)
        except psutil.Error as exc:
            log.error('Cannot inspect process: %s', exc.msg)
            sys.exit(1)

    # Handle the case where a PID command was provided on the command line. That
    # is, implement a PID-finding loop which indefinitely retries to resolve a
    # new PID once the current one seems to be invalid.
    while True:

        pid = pid_from_cmd(args.pid_command)

        if pid is None:
            # The PID command did not succeed. Try again in next iteration.
            time.sleep(PROCESS_PID_POLL_INTERVAL_SECONDS)
            continue

        try:
            sample_process(pid, samplequeue)
        except psutil.Error as exc:
            log.error('Cannot inspect process: %s', exc.msg)


def sample_process(pid, samplequeue):
    """
    Sample (periodically inspect) process with ID `pid`, and put each sample (a
    collection of values associated with a timestamp) into the `samplequeue`.
    """

    for sample in generate_samples(pid):
        # This writes the data to a buffer in the current process, and a
        # background thread communicates it through a pipe to the worker process
        # which outputs the data (to stdout or to disk).
        samplequeue.put(sample)


def generate_samples(pid):
    """
    Generator for generating samples. Each sample is a collection of values
    associated with a timestamp.

    This function is expected to raise `psutil.Error` (e.g. especially
    `psutil.NoSuchProcess`).
    """

    # Put `psutil` into local namespace (this does not re-import, is fast).
    import psutil

    # Prepare process analysis. This does not yet look at process metrics, but
    # already errors out when the PID is unknown.
    process = psutil.Process(pid)

    # `time1` is the reference timestamp for differential analysis where a time
    # difference is in the denominator of a calculation. Use a monotonic time
    # source for that instead of the system time, and then get the other data
    # points relevant for the differential analysis (CPU utilization)
    # immediately after getting `time1`.
    time1 = time.monotonic()
    ctime1 = process.cpu_times()

    time.sleep(PROCESS_SAMPLE_INTERVAL_SECONDS)

    while True:

        # `time2` and `cpu_times` must be collected immediately one after
        # another so that the below calculation of CPU utilization (differential
        # analysis with the time difference in the denominator) is as correct as
        # possible. Internally, `as_dict()` uses psutil's `oneshot()` context
        # manager for optimizing the process of collecting the requested data in
        # one go. The requested attributes are confirmed to be sampled quickly;
        # tested manually via the timeit module on not-so-well-performing
        # laptop:
        #
        # >>> import psutil
        # >>> from timeit import timeit
        # >>> attrs = ['cpu_times', 'io_counters', 'memory_info', 'num_fds']
        # >>> process = psutil.Process(pid=20844)
        # >>> timeit('process.as_dict(attrs)', number=100, globals=globals()) / 100
        # 0.0001447438000468537
        #
        # That is, these data are returned within less than a millisecond which
        # is absolutely tolerable
        time2 = time.monotonic()
        datadict = process.as_dict(
            attrs=['cpu_times', 'io_counters', 'memory_info', 'num_fds'])

        # Collect other system-wide data.
        # https://serverfault.com/a/85481/121951
        system_mem = psutil.virtual_memory()
        loadavg = os.getloadavg()

        ctime2 = datadict['cpu_times']
        proc_io = datadict['io_counters']
        proc_mem = datadict['memory_info']
        proc_num_fds = datadict['num_fds']

        # Current time with microsecond resolution, in UTC, ISO 8601 format.
        time_isostring = datetime.now().isoformat()

        # CPU-time related calculation.
        delta_realtime = time2 - time1
        delta_ctime_user = ctime2.user - ctime1.user
        delta_ctime_system = ctime2.system - ctime1.system
        delta_ctime_total = delta_ctime_user + delta_ctime_system
        proc_util_percent_total = 100 * delta_ctime_total / delta_realtime
        proc_util_percent_user = 100 * delta_ctime_user / delta_realtime
        proc_util_percent_system = 100 * delta_ctime_system / delta_realtime

        # Emit sample, an n-tuple.
        yield (
            time_isostring,
            # OS load averages:
            loadavg[0],
            loadavg[1],
            loadavg[2],
            # Basically the output of `free`, plus `available`. From psutil doc:
            # "The sum of used and available does not necessarily equal total"
            system_mem.available,
            system_mem.total,
            system_mem.used,
            system_mem.free,
            system_mem.shared,
            system_mem.buffers,
            system_mem.cached,
            # Some more system-wide mem stats:
            system_mem.active,
            system_mem.inactive,
            # The process ID of the currently monitored process, and process-
            # specific metrics. Before this there are only system-wide metrics.
            # The process ID can change over time.
            pid,
            proc_util_percent_total,
            proc_util_percent_user,
            proc_util_percent_system,
            proc_io.read_chars,
            proc_io.write_chars,
            proc_mem.rss,
            proc_mem.vms,
            proc_mem.dirty,
            proc_num_fds
        )

        # For differential values, store 'new values' as 'old values' for next
        # iteration.
        ctime1 = ctime2
        time1 = time2

        # Wait (approximately) for the configured sampling interval.
        time.sleep(PROCESS_SAMPLE_INTERVAL_SECONDS)


def pid_from_cmd(pid_command):
    """
    Example:

        pgrep --parent $(pgrep -u dcos_bouncer --parent 1)

    Explanation for example: first find the process(es) owned by user
    dcos_bouncer, being a direct child of process 1, and then get the child(ren)
    of this process/these processes. Knowing that there is precisely one parent
    process a single child thereof belonging to dcos_bouncer results in the
    process ID of the gunicorn worker process.

    Return `None` or integer.
    """

    # Execute command through shell to allow for powerful syntax. Redirect
    # stderr to stdout.
    p = subprocess.run(
        pid_command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        shell=True
    )

    # Decode data obtained from stdout/err in a failsafe manner.
    outtext = p.stdout.decode('ascii', errors='surrogateescape').strip()

    if p.returncode != 0:
        log.info('PID command returned non-zero')
        if outtext:
            log.debug('PID command stdout/err:\n%s', outtext)
        return None

    if not outtext:
        log.info('PID command did not return output')
        return None

    try:
        pid = int(outtext.strip())
    except ValueError:
        log.info('PID command returned unexpected stdout/err:\n%s', outtext)
        return None

    log.info('New process ID from PID command: %s', pid)

    return pid


if __name__ == "__main__":
    main()
