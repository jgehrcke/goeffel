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
purposes but the methodology around persisting the collected timeseries data is
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

References for interpreting output:

  Disk I/O statistics:

    - https://unix.stackexchange.com/a/462732 (What are merged writes?)
    - https://www.xaprb.com/blog/2010/01/09/how-linux-iostat-computes-its-results/
    - https://blog.serverfault.com/2010/07/06/777852755/ (interpreting iostat output)
    - https://stackoverflow.com/a/8512978 (what is %util in iostat?)
    - https://coderwall.com/p/utc42q/understanding-iostat
    - https://www.percona.com/doc/percona-toolkit/LATEST/pt-diskstats.html

  System memory statistics:

    - https://serverfault.com/a/85481/121951

  Other notes:

    - For writing the HDF5 file we could use pandas' pd.HDFStore implementation.
      However, already requiring pytables for measurement is a pretty big
      dependency to meet, and I am trying to get away w/o pandas in the
      measurement system itself.

    - https://cyrille.rossant.net/moving-away-hdf5/
    -
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
SAMPLE_INTERVAL_SECONDS = 0.5


PROCESS_PID_POLL_INTERVAL_SECONDS = 2.0


# Measure invocation time (is consumed in various places).
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

    # Note(JP): build a mode without process-specific monitoring (no pid, no pid
    # command)?

    # Note(JP): build a mode where disk IO stats are collected for all disks?

    # Note(JP): build in a concept that allows for "running this permanently",
    # by building a retention policy, rotating HDF5 files (and deleting old
    # ones), or something along these lines.

    # TODO(JP): measure performance impact of messer with another instance of
    # messer.

    what = parser.add_mutually_exclusive_group(required=True)

    what.add_argument(
        '--pid-command',
        metavar='\'PID COMMAND\'',
    )
    what.add_argument('--pid', metavar='PROCESSID', type=int)

    # Collect ideas for more flexible configuration.
    # what.add_argument('--diskstats', action='store_true')

    parser.add_argument(
        '--diskstats',
        action='append',
        metavar='DEVICENAME',
    )

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

    # Import here, delayed, instead of in the header (otherwise the command line
    # help / error reporting is noticeably slowed down).
    global psutil
    import psutil

    # Do custom argument processing.
    process_diskstats_args()

    # Set up the infrastructure for decoupling measurement (sample collection)
    # from persisting the data.
    samplequeue = multiprocessing.Queue()

    log.info(
        'Collect time series for the folling metrics: \n%s',
        '\n'.join(k for k in HDF5_SAMPLE_SCHEMA.keys()))

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
        log.info('Wait for producer buffer to be empty')
        samplequeue.join_thread()
        samplewriter.join()
        log.debug('Sample writer process terminated')


def process_diskstats_args():

    if not ARGS.diskstats:
        return

    known_devnames = list(psutil.disk_io_counters(perdisk=True).keys())
    for devname in ARGS.diskstats:
        if devname not in known_devnames:
            sys.exit('Invalid disk device name: %s\nAvailable names: %s' % (
                devname, ', '.join(known_devnames)))

        # Dynamically add columns to the HDF5 table schema.
        column_names = [
            'disk_' + devname + '_util_percent',
            'disk_' + devname + '_write_latency_ms',
            'disk_' + devname + '_read_latency_ms',
            'disk_' + devname + '_merged_read_rate_hz',
            'disk_' + devname + '_merged_write_rate_hz',
            'disk_' + devname + '_userspace_read_rate_hz',
            'disk_' + devname + '_userspace_write_rate_hz',
        ]

        # Note(JP): Windows support might be easy to add by conditionally
        # accounting for certain disk metrics.

        for n in column_names:
            hdf5_schema_add_column(colname=n, coltype=tables.Float32Col)


# This defines the initial set of columns, more columns can be added dynamically
# by the program, based on discovery or command line arguments.
HDF5_SAMPLE_SCHEMA = {
    # Store time in two formats: for flexible analyses store the unix time stamp
    # with subsecond precision, and for convenience it is good to also have a
    # string representation of the local time (also with subsecond precision)
    # stored.
    'unixtime': tables.Time64Col(pos=0),
    'isotime_local': tables.StringCol(26, pos=1),

    # System-wide metrics.
    'system_loadavg1': tables.Float16Col(pos=2),
    'system_loadavg5': tables.Float16Col(pos=3),
    'system_loadavg15': tables.Float16Col(pos=4),
    'system_mem_available': tables.UInt64Col(pos=5),
    'system_mem_total': tables.UInt64Col(pos=6),
    'system_mem_used': tables.UInt64Col(pos=7),
    'system_mem_free': tables.UInt64Col(pos=8),
    'system_mem_shared': tables.UInt64Col(pos=9),
    'system_mem_buffers': tables.UInt64Col(pos=10),
    'system_mem_cached': tables.UInt64Col(pos=11),
    'system_mem_active': tables.UInt64Col(pos=12),
    'system_mem_inactive': tables.UInt64Col(pos=13),

    # Process-specific metrics.
    'proc_pid': tables.Int32Col(pos=14),
    'proc_util_percent_total': tables.Float32Col(pos=15),
    'proc_util_percent_user': tables.Float32Col(pos=16),
    'proc_util_percent_system': tables.Float32Col(pos=17),
    'proc_io_read_throughput_mibps': tables.Float32Col(pos=18),
    'proc_io_write_throughput_mibps': tables.Float32Col(pos=19),
    'proc_io_read_rate_hz': tables.Float32Col(pos=20),
    'proc_io_write_rate_hz': tables.Float32Col(pos=21),
    'proc_mem_rss': tables.UInt64Col(pos=22),
    'proc_mem_vms': tables.UInt64Col(pos=23),
    'proc_mem_dirty': tables.UInt32Col(pos=24),
    'proc_num_fds': tables.UInt32Col(pos=25),
}


def hdf5_schema_add_column(colname, coltype):
    assert colname not in HDF5_SAMPLE_SCHEMA
    colpos = len(HDF5_SAMPLE_SCHEMA) + 1
    HDF5_SAMPLE_SCHEMA[colname] = coltype(pos=colpos)


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

    For the disk I/O operations performed here to not affect the main loop (as
    of disk write hiccups / latencies) this is being run in a separate process.
    That is, so the sampling (measurement) itself is decoupled from persisting
    the data through a Queue-based buffer (in the parent process) between both
    processes.
    """

    import signal
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    try:
        hostname = platform.node()
    except Exception as e:
        log.info('Cannot get system hostname: %s', e)
        hostname = ''

    log.info('Create HDF5 file: %s', ARGS.outfile_hdf5)

    # For choosing compression parameters, interoperability and reliability
    # (reducing risk for corruption) matters more than throughput.
    hdf5_compression_filter = tables.Filters(
        complevel=5, complib='zlib', fletcher32=True)

    hdf5file = tables.open_file(
        ARGS.outfile_hdf5,
        mode='w',
        title='messer.py time series file, invocation at ' + INVOCATION_TIME_LOCAL_STRING,
        filters=hdf5_compression_filter
    )

    csvfile = None
    csv_column_header_written = False
    if ARGS.outfile_csv:
        log.info('Create CSV file: %s', ARGS.outfile_csv)
        csvfile = open(ARGS.outfile_csv, 'wb')
        csvfile.write(b'# messer_timeseries\n')
        csvfile.write(b'# %s\n' (INVOCATION_TIME_LOCAL_STRING, ))
        csvfile.write(b'# system hostname: %s\n' (hostname, ))
        csvfile.write(b'# schema version: %s\n' (MESSER_SAMPLE_SCHEMA_VERSION, ))

    # Do not use HDF5 groups. Write a single table per file, with a well-known
    # table name so that the analysis program can discover.
    # Note(JP): investicate chunk size, and generally explore providing an
    # expected row count. For example, with a 1 Hz sampling rate aboout 2.5 million
    # rows are collected within 30 days:
    # >>> 24 * 60 * 60 * 30
    # 2592000
    #
    hdf5table = hdf5file.create_table(
        where='/',
        name='messer_timeseries',
        description=HDF5_SAMPLE_SCHEMA,
        title='messer.py time series, invocation at ' + INVOCATION_TIME_LOCAL_STRING,
    )

    # Use so-called HDF5 table user attributes for storing relevant metadata.
    hdf5table.attrs.invocation_time_unix = INVOCATION_TIME_UNIX_TIMESTAMP
    hdf5table.attrs.invocation_time_local = INVOCATION_TIME_LOCAL_STRING
    hdf5table.attrs.system_hostname = hostname
    hdf5table.attrs.messer_schema_version = MESSER_SAMPLE_SCHEMA_VERSION
    # Store both, pid and pid command although we know only one is populated.
    hdf5table.attrs.messer_pid_command = ARGS.pid_command
    hdf5table.attrs.messer_pid = ARGS.pid
    hdf5table.attrs.messer_sampling_interval_seconds = SAMPLE_INTERVAL_SECONDS


    # The pytables way of doing things: "The row attribute of table points to
    # the Row instance that will be used to write data rows into the table. We
    # write data simply by assigning the Row instance the values for each row as
    # if it were a dictionary (although it is actually an extension class),
    # using the column names as keys".

    while True:
        sample = queue.get()

        # `sample` is expected to be an n-tuple or None. Shut down worker
        # process in case of `None`.
        if sample is None:
            log.debug('Sample writer process: shutting down')
            break

        time_before_flush = time.monotonic()

        # Simulate I/O slowness.
        # import random
        # time.sleep(random.randint(1, 10) / 30.0)

        _write_sample_hdf5(sample, hdf5table)

        # Note(JP): how can we make it so that after this line the HDF5 file is
        # *valid* and can be opened by other tooling? 'NoneType' object has no
        # attribute 'get_node'
        # 2019-05-28 18:27:57,355 - vitables.h5db.dbstreemodel - ERROR - Opening cancelled: file /home/jp/dev/messer/messer_timeseries_20190528_182722.hdf5 already open.
        _write_sample_csv(sample, csvfile)

        sample_write_latency = time.monotonic() - time_before_flush

        log.debug('Wrote sample to file(s) within %.6f s', sample_write_latency)


def _write_sample_hdf5(sample,  hdf5table):

        for key, value in sample.items():
            hdf5table.row[key] = value

        # Write sample to pytable's table I/O buffer. Then flush to disk (early
        # and often -- with the small data output rate it's not expected that
        # frequent flushing creates a bottleneck).
        hdf5table.row.append()
        hdf5table.flush()


def _write_sample_csv(sample, csvfile):
    if csvfile is not None:

        samplevalues = tuple(v for k, v in sample.items())

        # Apply a bit of custom formatting.
        # Note(JP): store these format strings closer to the column listing,
        # and assemble a format string dynamically.
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


def mainloop(samplequeue, samplewriter):
    """
    Wraps the main sampling loop. Is intended to be interruptible via SIGINT /
    Ctrl+C, and therefore wrapped in a `KeyboardInterrupt` exception handler
    in the caller.
    """

    # Handle case where a specific (constant) PID was provided on the command
    # line. Error out and exit program in the moment the PID cannot be monitored
    # (as of process not existing, insufficient permissions, etc.).
    if ARGS.pid is not None:
        try:
            sample_process(ARGS.pid, samplequeue, samplewriter)
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
        # which outputs the data (to stdout or to disk). Note(JP): the feeder
        # thread buffer is of limited size and it's not configurable via an
        # official interface as far as I see. It might be advisable to prepare
        # for backpressure by applying some timeout control, and by potentially
        # wrapping the put() call in a control loop.
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

    # `t_rel1` is the reference timestamp for differential analysis where a time
    # difference is in the denominator of a calculation:
    #
    #           V2 - V1
    #        --------------
    #        t_rel2 - t_rel1
    #
    #
    # Use a monotonic time source for measuring t_rel1 (and later t_rel2)
    # instead of the system time and then get the first set of data points
    # relevant for the differential analysis immediately after getting t_rel1.
    t_rel1 = time.monotonic()
    procstats1 = process.as_dict(attrs=['cpu_times', 'io_counters', 'memory_info', 'num_fds'])
    diskstats1, diskstats2 = None, None
    if ARGS.diskstats:
        diskstats1 = psutil.disk_io_counters(perdisk=True)

    time.sleep(SAMPLE_INTERVAL_SECONDS)

    while True:

        # `t_rel2` and `cpu_times` must be collected immediately one after
        # another in the same order as before so that the below calculation of
        # CPU utilization (differential analysis with the time difference in the
        # denominator) is as correct as possible. Internally, `as_dict()` uses
        # psutil's `oneshot()` context manager for optimizing the process of
        # collecting the requested data in one go. The requested attributes are
        # confirmed to be sampled quickly; tested manually via the timeit module
        # on a not-so-well-performing laptop:
        #
        # >>> import psutil
        # >>> from timeit import timeit
        # >>> attrs = ['cpu_times', 'io_counters', 'memory_info', 'num_fds']
        # >>> process = psutil.Process(pid=20844)
        # >>> timeit('process.as_dict(attrs)', number=100, globals=globals()) / 100
        # 0.0001447438000468537
        #
        # That is, these data are returned within less than a millisecond which
        # is absolutely tolerable.
        #
        # `psutil.disk_io_counters()` is similarly fast on my test machine
        # >>> timeit('psutil.disk_io_counters(perdisk=True)', number=100, globals=globals()) / 100
        # 0.0002499251801054925
        #
        # Do not do this by default though because on systems with a complex
        # disk setup I am not sure if it's wise to collect all disk stats
        # by default.

        t_rel2 = time.monotonic()
        procstats2 = process.as_dict(attrs=['cpu_times', 'io_counters', 'memory_info', 'num_fds'])
        if ARGS.diskstats:
            diskstats2 = psutil.disk_io_counters(perdisk=True)

        # Now the data for the timing-sensitive differential anlysis has been
        # acquired. What follows is the acquisition of system-wide metrics.

        # https://serverfault.com/a/85481/121951
        system_mem = psutil.virtual_memory()
        loadavg = os.getloadavg()

        # Measure and log duration of system interaction for debugging purposes.
        t_rel_for_debug = time.monotonic()
        log.debug('Data acquisition took %.6f s', t_rel_for_debug - t_rel2)

        # All data for this sample has been acquired (what follows is data
        # mangling, but no calls to `os` or `psutil` anymore). Get the current
        # time T with microsecond resolution and set it as the time that this
        # sample has been taken. For those values that are built by differential
        # analysis it means that the value corresponds to the time interval
        # [T_previous - P, T_current], with P being the sample period [s]. Use
        # the system time here because it's what's most useful even during clock
        # drift (P is measured using a monotonic clock source, and during clock
        # drift T_previous - T_current is not equal to P).
        time_sample_timestamp = time.time()
        time_sample_isostring_local = datetime.fromtimestamp(
            time_sample_timestamp).isoformat()

        # Build CPU utilization values (average CPU utilization within the past
        # sampling interval).
        cputimes1 = procstats1['cpu_times']
        cputimes2 = procstats2['cpu_times']

        proc_io1 = procstats1['io_counters']
        proc_io2 = procstats2['io_counters']

        delta_t = t_rel2 - t_rel1
        _delta_cputimes_user = cputimes2.user - cputimes1.user
        _delta_cputimes_system = cputimes2.system - cputimes1.system
        _delta_cputimes_total = _delta_cputimes_user + _delta_cputimes_system
        proc_util_percent_total = 100 * _delta_cputimes_total / delta_t
        proc_util_percent_user = 100 * _delta_cputimes_user / delta_t
        proc_util_percent_system = 100 * _delta_cputimes_system / delta_t

        proc_mem = procstats2['memory_info']
        proc_num_fds = procstats2['num_fds']

        # Calculate disk I/O statistics
        disksampledict = calc_diskstats(delta_t, diskstats1, diskstats2)

        # Calculate I/O statistics for the process. Instead of bytes per second
        # calculate througput as MiB per second, whereas 1 MiB is 1024 * 1024
        # bytes = 1048576 bytes. Note(JP): I did this because by default plots
        # with bytes per second are hard to interpret. However, in other
        # scenarios others might think that bps are better/more flexible. No
        # perfect solution.
        proc_io_read_throughput_mibps = (proc_io2.read_chars - proc_io1.read_chars) / 1048576.0 / delta_t
        proc_io_write_throughput_mibps = (proc_io2.write_chars - proc_io1.write_chars) / 1048576.0 / delta_t
        proc_io_read_rate_hz = (proc_io2.read_count - proc_io1.read_count) / delta_t
        proc_io_write_rate_hz = (proc_io2.write_count - proc_io1.write_count) / delta_t

        # Order matters, but only for the CSV output in the sample writer.
        sampledict = OrderedDict((
            ('unixtime', time_sample_timestamp),
            ('isotime_local', time_sample_isostring_local),
            ('system_loadavg1', loadavg[0]),
            ('system_loadavg5', loadavg[1]),
            ('system_loadavg15', loadavg[2]),
            ('system_mem_available', system_mem.available),
            ('system_mem_total', system_mem.total),
            ('system_mem_used', system_mem.used),
            ('system_mem_free', system_mem.free),
            ('system_mem_shared', system_mem.shared),
            ('system_mem_buffers', system_mem.buffers),
            ('system_mem_cached', system_mem.cached),
            ('system_mem_active', system_mem.active),
            ('system_mem_inactive', system_mem.inactive),
            ('proc_pid', pid),
            ('proc_util_percent_total', proc_util_percent_total),
            ('proc_util_percent_user', proc_util_percent_user),
            ('proc_util_percent_system', proc_util_percent_system),
            ('proc_io_read_throughput_mibps', proc_io_read_throughput_mibps),
            ('proc_io_write_throughput_mibps', proc_io_write_throughput_mibps),
            ('proc_io_read_rate_hz', proc_io_read_rate_hz),
            ('proc_io_write_rate_hz', proc_io_write_rate_hz),
            ('proc_mem_rss', proc_mem.rss),
            ('proc_mem_vms', proc_mem.vms),
            ('proc_mem_dirty', proc_mem.dirty),
            ('proc_num_fds', proc_num_fds),
        ))

        # Add disk sample data to sample dict.
        for k, v in disksampledict.items():
            sampledict[k] = v

        # For differential values, store 'new values' as 'old values' for next
        # iteration.
        procstats1 = procstats2
        #cputimes1 = cputimes2
        diskstats1 = diskstats2
        t_rel1 = t_rel2

        # provide the newly acquired sample to the consumer of this generator.
        yield sampledict

        # Wait (approximately) for the configured sampling interval.
        time.sleep(SAMPLE_INTERVAL_SECONDS)


def calc_diskstats(delta_t, s1, s2):
    """
    `delta_t` must have unit seconds.

    `s1` and `s2` must each be an object returned by
    `psutil.disk_io_counters()`, one at time t1, and the other at the (later)
    time t2, with t2 - t2 = delta_t.
    """

    sampledict = OrderedDict()

    for dev in ARGS.diskstats:

        # Attempt to implements iostat's `%util` metric which is documented with
        # "Percentage of elapsed time during which I/O requests were issued  to
        # the  device  (bandwidth  utilization  for the device)."
        # Calculate disk utilization from `busy_time` which is documented by
        # psutil with "time spent doing actual I/Os (in milliseconds)". Build
        # the ratio between the actual time elapsed and `busy_time`, express it
        # in percent. Percent calc yields factor 100, millisecond conversion
        # yields factor 1000, leaving behind an overall factor 10.
        sampledict['disk_' + dev + '_util_percent'] = \
            (s2[dev].busy_time - s1[dev].busy_time) / (delta_t * 10)

        # Implement iostat's `w_await` which is documented with "The average
        # time (in  milliseconds)  for  write requests issued to the device to
        # be served. This includes the time spent by the requests in queue and
        # the time spent servicing them".
        #
        # Also see https://www.kernel.org/doc/Documentation/iostats.txt
        #
        # Use psutil's `write_time` which is documented with "time spent writing
        # to disk (in milliseconds)", extracted from field 8 in /proc/diskstats,
        # see. And use psutil's `write_count` which is extracted from field 5 in
        # /proc/diststats. Notably, it is *not* the merged write count, but the
        # user space write count. Which seems to be what iostat uses for
        # calculating w_await.
        #
        # In an experiment I have seen that the following can happen within a
        # second of real time: (observed via `iostat -x 1 | grep xvdh` and via
        # direct observation of /proc/diststats): 3093.00 userspace write
        # requests served, merged into 22.00 device write requests, yielding a
        # total of 120914 milliseconds "spent writing", resulting in an
        # "average" write latency of 25 milliseconds. But what do the 25
        # milliseconds really mean here? On average, humans have less than two
        # legs ...
        #
        # Well, for now, I am happy that the current implementation method
        # re-creates the iostat output, which is the goal.
        #
        # About the case where there were no reads or writes during the sampling
        # interval: emitting a latency of '0' is misleading. It's common to
        # store a NaN/Null/None in that case, but that's not easily supported by
        # the HDF5 file format. Instead, store -1, with the convention that this
        # value means precisely "no writes/reads happened here.
        delta_write_count = s2[dev].write_count - s1[dev].write_count
        if delta_write_count == 0:
            avg_write_latency_ms = -1
        else:
            avg_write_latency_ms = (s2[dev].write_time - s1[dev].write_time) / delta_write_count
        sampledict['disk_' + dev + '_write_latency_ms'] = avg_write_latency_ms

        delta_read_count = s2[dev].read_count - s1[dev].read_count
        if delta_read_count == 0:
            avg_read_latency_ms = -1
        else:
            avg_read_latency_ms = (s2[dev].read_time - s1[dev].read_time) / delta_read_count
        sampledict['disk_' + dev + '_read_latency_ms'] = avg_read_latency_ms

        # ## IO request rate, merged by kernel (what the device sees).
        #
        # On Linux what matters more than the user space read or write request
        # rate, is the _merged_ read or write request rate (the kernel attempts
        # to merge individual user space requests before passing them to the
        # hardware). For non-random I/O patterns this greatly reduces the of
        # individual reads and writes issed to disk.
        sampledict['disk_' + dev + '_merged_write_rate_hz'] = \
            (s2[dev].write_merged_count - s1[dev].write_merged_count) / delta_t
        sampledict['disk_' + dev + '_merged_read_rate_hz'] = \
            (s2[dev].read_merged_count - s1[dev].read_merged_count) / delta_t

        # ## IO request rate emitted by user space.
        sampledict['disk_' + dev + '_userspace_write_rate_hz'] = \
            delta_write_count / delta_t
        sampledict['disk_' + dev + '_userspace_read_rate_hz'] = \
            delta_read_count / delta_t

    return sampledict


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
