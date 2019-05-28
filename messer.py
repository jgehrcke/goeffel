#!/usr/bin/env python
#
# MIT License

# Copyright (c) 2018-2019 Dr. Jan-Philip Gehrcke

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
import platform
import subprocess
import multiprocessing

from collections import OrderedDict
from datetime import datetime

import tables

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


PROCESS_SAMPLE_INTERVAL_SECONDS = 0.2
PROCESS_PID_POLL_INTERVAL_SECONDS = 2.0

# The invocation time is used in various places.
INVOCATION_TIME_UNIX_TIMESTAMP = time.time()
INVOCATION_TIME_LOCAL_STRING = datetime.fromtimestamp(
    INVOCATION_TIME_UNIX_TIMESTAMP).strftime(format='%Y%m%d_%H%M%S')


MESSER_SAMPLE_SCHEMA_VERSION = 1


# Will be populated with options from command line arguments.
ARGS = None


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

    # TODO: make it so that at least one output file method is defined.
    # maybe: require HDF5 to work.
    # TODO: enable optional CSV output, in  addition.
    parser.add_argument(
        '--outfile-hdf5',
        metavar='PATH',
        default='messer_timeseries_' + INVOCATION_TIME_LOCAL_STRING + '.hdf5',
    )

    parser.add_argument(
        '--outfile-csv',
        metavar='PATH',
        default=None,
    )

    global ARGS
    ARGS = parser.parse_args()

    # Set up the infrastructure for decoupling measurement (sample collection)
    # from persisting the data.
    samplequeue = multiprocessing.Queue()
    samplewriter = multiprocessing.Process(
        target=sample_writer_process,
        args=(samplequeue,)
    )
    samplewriter.start()

    try:
        mainloop(samplequeue, samplewriter)
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


class MesserDataSample(tables.IsDescription):
    # Use the `pos` argument for defining the order of columns in the HDF5
    # file.
    unixtime = tables.Time64Col(pos=0)
    isotime_local = tables.StringCol(26,pos=1)
    loadavg1 = tables.Float16Col(pos=2)
    loadavg5 = tables.Float16Col(pos=3)
    loadavg15 = tables.Float16Col(pos=4)
    system_mem_available = tables.UInt64Col(pos=5)
    system_mem_total = tables.UInt64Col(pos=6)
    system_mem_used = tables.UInt64Col(pos=7)
    system_mem_free = tables.UInt64Col(pos=8)
    system_mem_shared = tables.UInt64Col(pos=9)
    system_mem_buffers = tables.UInt64Col(pos=10)
    system_mem_cached = tables.UInt64Col(pos=11)
    system_mem_active = tables.UInt64Col(pos=12)
    system_mem_inactive = tables.UInt64Col(pos=13)
    pid = tables.Int32Col(pos=14)
    proc_util_percent_total = tables.Float32Col(pos=15)
    proc_util_percent_user = tables.Float32Col(pos=16)
    proc_util_percent_system = tables.Float32Col(pos=17)
    proc_io_read_chars = tables.UInt64Col(pos=18)
    proc_io_write_chars = tables.UInt64Col(pos=19)
    proc_mem_rss = tables.UInt64Col(pos=20)
    proc_mem_vms = tables.UInt64Col(pos=21)
    proc_mem_dirty = tables.UInt32Col(pos=22)
    proc_num_fds = tables.UInt32Col(pos=23)


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

    log.info('Create HDF5 file: %s', ARGS.outfile_hdf5)

    # Interoperability and reliability (reducing risk for corruption) matters,
    # throughput does not matter so much.
    hdf5_compression_filter = tables.Filters(
        complevel=5, complib='zlib', fletcher32=True)

    hdf5file = tables.open_file(
        ARGS.outfile_hdf5,
        mode='w',
        title='messer.py time series file, invocation at ' + INVOCATION_TIME_LOCAL_STRING,
        filters=hdf5_compression_filter
    )

    # Note(JP): add messer schema version into the file?
    # machine hostname?

    csvfile = None
    csv_column_header_written = False
    if ARGS.outfile_csv:
        log.info('Create CSV file: %s', ARGS.outfile_csv)
        csvfile = open(ARGS.outfile_csv, 'wb')
        # TODO: Write comments into header.
        # Like messer version, time, ...


    # No hierarchy, invariant: just a single table per file, with a well-known
    # table name so that the analysis program can discover.
    hdf5table = hdf5file.create_table(
        where='/',
        name='messer_timeseries',
        description=MesserDataSample,
        title='messer.py time series, invocation at ' + INVOCATION_TIME_LOCAL_STRING,
    )

    # Set user attributes for storing relevant metadata.
    hdf5table.attrs.invocation_time_unix = INVOCATION_TIME_UNIX_TIMESTAMP
    hdf5table.attrs.invocation_time_local = INVOCATION_TIME_LOCAL_STRING

    try:
        hostname = platform.node()
    except Exception as e:
        log.info('Cannot get system hostname: %s', e)
        hostname = ''

    hdf5table.attrs.system_hostname = hostname
    hdf5table.attrs.messer_schema_version = MESSER_SAMPLE_SCHEMA_VERSION
    hdf5table.attrs.messer_pid_command = ARGS.pid_command

    # The pytables way of doing things: "The row attribute of table points to
    # the Row instance that will be used to write data rows into the table. We
    # write data simply by assigning the Row instance the values for each row as
    # if it were a dictionary (although it is actually an extension class),
    # using the column names as keys".

    hdf5samplerow = hdf5table.row

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

        time_before_flush = time.monotonic()

        for key, value in sample.items():
            hdf5samplerow[key] = value

        # Write sample to pytable's table I/O buffer. Then flush to disk (early
        # and often -- with the small data output rate it's not expected that
        # frequent flushing creates a bottleneck).
        hdf5samplerow.append()
        hdf5table.flush()

        # Note(JP): how can we make it so that after this line the HDF5 file is
        # *valid* and can be opened by other tooling? 'NoneType' object has no
        # attribute 'get_node'
        # 2019-05-28 18:27:57,355 - vitables.h5db.dbstreemodel - ERROR - Opening cancelled: file /home/jp/dev/messer/messer_timeseries_20190528_182722.hdf5 already open.

        if csvfile is not None:

            samplevalues = tuple(v for k, v in sample.items())

            # Apply a bit of custom formatting.
            csv_sample_row = (
                '%.6f, %s, %5.2f, %5.2f, %5.2f, '
                '%d, %d, %d, %d, %d, %d, %d, %d, %d, '
                '%d, %5.2f, %5.2f, %5.2f, %d, %d, %d, %d, %d, %d\n' % samplevalues
            )

            # Write the column header to the CSV file if not yet done. Do that
            # here so that the column names can be explicitly read from an
            # (ordered) sample dicitionary.
            if not csv_column_header_written:
                csvfile.write(
                    ','.join(k for k in sample.keys()).encode('ascii') + b'\n')
                csv_column_header_written = True

            csvfile.write(csv_sample_row.encode('ascii'))
            csvfile.flush()

        sample_write_latency = time.monotonic() - time_before_flush

        log.debug('Wrote sample to file(s) within %.6f s', sample_write_latency)


def mainloop(samplequeue, samplewriter):
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
    if ARGS.pid is not None:
        try:
            sample_process(ARGS.pid, samplequeue)
        except psutil.Error as exc:
            log.error('Cannot inspect process: %s', exc.msg)
            sys.exit(1)

    # Handle the case where a PID command was provided on the command line. That
    # is, implement a PID-finding loop which indefinitely retries to resolve a
    # new PID once the current one seems to be invalid.
    while True:

        pid = pid_from_cmd(ARGS.pid_command)

        if pid is None:
            # The PID command did not succeed. Try again in next iteration.
            time.sleep(PROCESS_PID_POLL_INTERVAL_SECONDS)
            continue

        try:
            sample_process(pid, samplequeue, samplewriter)
        except psutil.Error as exc:
            log.error('Cannot inspect process: %s', exc.msg)


def sample_process(pid, samplequeue, samplewriter):
    """
    Sample (periodically inspect) process with ID `pid`, and put each sample (a
    collection of values associated with a timestamp) into the `samplequeue`.
    """

    for sample in generate_samples(pid):

        # TODO(JP): if the sample writer crashes we want to crash the entire
        # program.

        if not samplewriter.is_alive():
            sys.exit('The sample writer process terminated. Exit.')

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

        # Current time with microsecond resolution, in UTC, ISO 8601 format,
        # local time. Also add a UTC timestamp column.
        time_now_timestamp = time.time()
        time_now_isostring_local = datetime.fromtimestamp(time_now_timestamp).isoformat()

        # CPU-time related calculation.
        delta_realtime = time2 - time1
        delta_ctime_user = ctime2.user - ctime1.user
        delta_ctime_system = ctime2.system - ctime1.system
        delta_ctime_total = delta_ctime_user + delta_ctime_system
        proc_util_percent_total = 100 * delta_ctime_total / delta_realtime
        proc_util_percent_user = 100 * delta_ctime_user / delta_realtime
        proc_util_percent_system = 100 * delta_ctime_system / delta_realtime

        # Order matters, but only for the CSV output in the sample writer.
        sampledict = OrderedDict((
            ('unixtime', time_now_timestamp),
            ('isotime_local', time_now_isostring_local),
            ('loadavg1', loadavg[0]),
            ('loadavg5', loadavg[1]),
            ('loadavg15', loadavg[2]),
            ('system_mem_available', system_mem.available),
            ('system_mem_total', system_mem.total),
            ('system_mem_used', system_mem.used),
            ('system_mem_free', system_mem.free),
            ('system_mem_shared', system_mem.shared),
            ('system_mem_buffers', system_mem.buffers),
            ('system_mem_cached', system_mem.cached),
            ('system_mem_active', system_mem.active),
            ('system_mem_inactive', system_mem.inactive),
            ('pid', pid),
            ('proc_util_percent_total', proc_util_percent_total),
            ('proc_util_percent_user', proc_util_percent_user),
            ('proc_util_percent_system', proc_util_percent_system),
            ('proc_io_read_chars', proc_io.read_chars),
            ('proc_io_write_chars', proc_io.write_chars),
            ('proc_mem_rss', proc_mem.rss),
            ('proc_mem_vms', proc_mem.vms),
            ('proc_mem_dirty', proc_mem.dirty),
            ('proc_num_fds', proc_num_fds),
        ))
        yield sampledict

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
