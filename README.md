# Overview

Measures the resource utilization of a specific process over time.

Also measures the utilization / saturation of system-wide resources: this helps
putting the process-specific metrics into context.

Built for Linux. Windows and Mac OS support might come.

For a list of the currently supported metrics see [below](#measurands).

The name, [Göffel](https://de.wikipedia.org/wiki/Essbesteck#Mischformen), is the
German version of [spork](https://en.wikipedia.org/wiki/Spork):

![image of a spork](docs/figs/spork.jpg?raw=true "image of spork / Göffel")

Convenient, right?

## Highlights

- High sampling rate: the default sampling interval of `0.5 s` makes narrow
  spikes visible.
- Can monitor a program subject to process ID changes (useful for longevity
  experiments where the monitored process occasionally restarts, for instance as
  of fail-over scenarios).
- Is meant to run unsupervised. Has predictable disk space requirements (output
  file rotation and retention policy).
- Helps keeping data organized: time series data is written into
  [HDF5](https://en.wikipedia.org/wiki/Hierarchical_Data_Format) files, and
  annotated with relevant metadata such as the program invocation time, system
  hostname, and Goeffel software version.
- Comes with a data plotting tool separate from the data acquisition program.
- Values measurement correctness very highly (see [technical
  notes](#technical-notes)).


# CLI usage

## `goeffel`: data acquisition



## `goeffel-analysis`: data inspection and visualization

**Note**: `goeffel-analysis` provides an opinionated and limited approach to
visualizing data. For advanced and thorough data analysis I recommend building a
custom (maybe even ad-hoc!) data analysis pipeline using `pandas` and
`matplotlib` (or using the tooling of your choice).

**Warning**: The command line interface provided by `goeffel-analysis`,
especially for the plot commands, might change in the future. Suggestions for
improvement are welcome.

### `goeffel-analysis inspect`:

Use `goeffel-analysis inspect <path-to-HDF5-file>` for inspecting the contents
of a Goeffel HDF5 file. Example:

```text
goeffel-analysis inspect mwst18-master1-journal_20190801_111952.hdf5
Measurement meta data:
  System hostname: int-master1-mwt18.scaletesting.mesosphe.re
  Invocation time (local): 20190801_111952
  PID command: pgrep systemd-journal
  PID: None
  Sampling interval: 1.0 s

Table properties:
  Number of rows: 24981
  Number of columns: 38
  Number of data points (rows*columns): 9.49E+05
  First row's (local) time: 2019-08-01T11:19:53.613377
  Last  row's (local) time: 2019-08-01T18:52:49.954582
  Time span: 7h 32m 56s

Column names:
  unixtime
  ... snip ...
  system_mem_inactive
```

### `goeffel-analysis magic`: quickly plot data from a single time series file

The `goeffel-analysis magic <path-to-hdf5-file>` command plots a pre-selected
set of metrics in an opinionated way. More metrics can be added to the plot with
the `--metric <metric-name>` option. Example command:

```bash
goeffel-analysis magic \
  mwst18-master2-mesosmaster_20190801_112136.hdf5 \
  --metric proc_num_ip_sockets_open
```
Example output figure:
![goeffel-analysis magic example output image](docs/figs/analysis_magic_example_small.png?raw=true "goeffel-analysis magic example")

### `goeffel-analysis plot`: generic plot command

Can be used for example for comparing multiple time series. Say you have
monitored the same program across multiple replicas in a distributed system and
would like to compare the time evolution of a certain metric across these
replicas. Then the `goeffel-analysis plot` command is here to help, invoked with
multiple `--series` arguments:

```bash
goeffel-analysis plot \
  --series mwst18-master1-journal_20190801_111952.hdf5 master1 \
  --series mwst18-master2-journal_20190801_112136.hdf5 master2 \
  --series mwst18-master3-journal_20190801_112141.hdf5 master3 \
  --series mwst18-master4-journal_20190801_112151.hdf5 master4 \
  --series mwst18-master5-journal_20190801_112157.hdf5 master5 \
  --column proc_cpu_util_percent_total \
      'CPU util (total) / %' \
      'systemd journal CPU utilization ' 15 \
  --subtitle 'MWST18, measured with Goeffel' \
  --legend-loc 'upper center'

```

Example output figure:
![goeffel-analysis plot example output image](docs/figs/analysis_plot_example_small.png?raw=true "goeffel-analysis plot example")


# Background and details

## Prior art

This was born out of a need for solid tooling. We started with [pidstat from
sysstat](https://github.com/sysstat/sysstat/blob/master/pidstat.c), launched as
`pidstat -hud -p $PID 1 1`. We found that it does not properly account for
multiple threads running in the same process, and that various issues in that
regard exist in this program across various versions (see
[here](https://github.com/sysstat/sysstat/issues/73#issuecomment-349946051),
[here](https://github.com/sysstat/sysstat/commit/52977c479), and
[here](https://github.com/sysstat/sysstat/commit/a63e87996)).

The program [cpustat](https://github.com/uber-common/cpustat) open-sourced by
Uber has a delightful README about the general measurement methodology and
overall seems to be a great tool. However, it seems to be optimized for
interactive usage (whereas we were looking for a robust measurement program
which can be pointed at a process and then be left unattended for a significant
while) and there does not seem to be a well-documented approach towards
persisting the collected time series data on disk for later inspection.

The program [psrecord](https://github.com/astrofrog/psrecord) (which effectively
wraps [psutil](https://psutil.readthedocs.io/en/latest/)) has a similar
fundamental approach as Goeffel; it however only , and
it does not have a clear separation of concerns between persisting the data to
disk, performing the measurement itself, and analyzing/plotting the data.

## Technical notes

- The core sampling loop does little work besides the measurement itself: it
  writes each sample to a queue. A separate process consumes this queue and
  persists the time series data to disk, for later inspection. This keeps the
  sampling rate predictable upon disk write latency spikes, or generally upon
  backpressure. This matters especially in cloud environments where we sometimes
  see fsync latencies of multiple seconds.

- Goeffel tries to not asymmetrically hide measurement uncertainty. For example,
  you might see it measure a CPU utilization of a single-threaded process
  slightly larger than 100 %. That's simply the measurement error. In other
  tooling such as `sysstat` it seems to be common practice to _asymmetrically_
  hide measurement uncertainty by capping values when they are known to in
  theory not exceed a certain threshold
  ([example](https://github.com/sysstat/sysstat/commit/52977c479d3de1cb2535f896273d518326c26722)).

- `goeffel` must be run with `root` privileges.

- The value `-1` has a special meaning for some metrics
  ([NaN](https://en.wikipedia.org/wiki/NaN), which cannot be represented
  properly in HDF5). Example: A disk write latency of `-1 ms` means that no
  write happened in the corresponding time interval.

- The highest meaningful sampling rate is limited by the kernel's timer and
  bookkeeping system.

# Measurands

[Measurand](https://en.wiktionary.org/wiki/measurand) is a word! This section
attempts to describe the individual data columns ("metrics"), their units, and
their meaning. There are four main categories:

- [Timestamps](#timestamps)
- [Process-specific metrics](#process-specific-metrics)
- [Disk metrics](#disk-metrics)
- [System-wide metrics](#system-wide-metrics)


### Timestamps

#### `unixtime`, `isotime_local`, `monotime`

The timestamp corresponding to the *right* boundary of the sampled time
interval.

* `unixtime` encodes the wall time. It is a canonical Unix timestamp (seconds
  since epoch, double precision floating point number); with sub-second
  precision and no timezone information. This is compatible with a wide range of
  tooling and therefore the general-purpose timestamp column for time series
  analysis (also see [How to convert the `unixtime` column into a
  `pandas.DatetimeIndex`](#how-to-convert-the-unixtime-column-into-a-pandasdatetimeindex)).
  **Note**: this is subject to system clock drift. In extreme cases this might
  go backwards, have jumps, and be a useless metric. In that case the `monotime`
  metric helps (see below).

* `isotime_local` is a human-readable version of the same timestamp as stored in
  `unixtime`. It is a 26 character long text representation of the *local* time
  using an ISO 8601 notation (and therefore also machine-readable). Like
  `unixtime` this metric is subject to system clock drift and might become
  pretty useless in extreme cases.

* `monotime` is based on a so-called
  [monotonic](https://www.python.org/dev/peps/pep-0418/#id19) clock source which
  is *not* subject to (accidental or well-intended) system clock drift. This
  column encodes most accurately the relative time difference between any two
  samples in the time series. The timestamps encoded in this column only make
  sense relative to each other; the difference between any two values in this
  column is a *wall time* difference in seconds, with sub-second precision.

### Process-specific metrics

#### `proc_pid`

The process ID of the monitored process. It can change if Goeffel was invoked
with the `--pid-command` option.

Momentary state at sampling time.

#### `proc_cpu_util_percent_total`

The CPU utilization of the process in `percent`.

Mean over the past sampling interval.

If the inspected process is known to contain just a single thread then this can
still sometimes be larger than 100 % as of measurement errors. If the process
runs more than one thread then this can go far beyond 100 %.

This is based on the sum of the time spent in user space and in kernel space.
For a more fine-grained picture the following two metrics are also available:
`proc_cpu_util_percent_user`, and `proc_cpu_util_percent_system`.

#### `proc_cpu_id`

The ID of the CPU that this process is currently running on.

Momentary state at sampling time.

#### `proc_ctx_switch_rate_hz`

The rate of ([voluntary and
involuntary](https://unix.stackexchange.com/a/442991)) context switches in `Hz`.

Mean over the past sampling interval.

#### `proc_num_threads`

The number of threads in the process.

Momentary state at sampling time.

#### `proc_num_ip_sockets_open`

The number of sockets currently being open. This includes IPv4 and IPv6 and does
not distinguish between TCP and UDP, and the connection state also does not
matter.

Momentary state at sampling time.

#### `proc_num_fds`

The number of file descriptors currently opened by this process.

Momentary state at sampling time.

#### `proc_disk_read_throughput_mibps` and `proc_disk_write_throughput_mibps`

The disk I/O throughput of the inspected process, in `MiB/s`.

Based on Linux' `/proc/<pid>/io` `rchar` and `wchar`. Relevant
[Linux kernel documentation](https://github.com/torvalds/linux/blob/33920f1ec5bf47c5c0a1d2113989bdd9dfb3fae9/Documentation/filesystems/proc.txt#L1609) (emphasis mine):

> `rchar`: The number of bytes which this task has caused to be read from
> storage. This is simply the sum of bytes which this process passed to read()
> and pread(). *It includes things like tty IO* and it is unaffected by whether
> or not actual physical disk IO was required (*the read might have been
> satisfied from pagecache*).

> `wcar`: The number of bytes which this task has caused, or shall cause to be
> written to disk. Similar caveats apply here as with rchar.

Mean over the past sampling interval.

#### `proc_disk_read_rate_hz` and `proc_disk_write_rate_hz`

The rate of read/write system calls issued by the process as inferred from the
Linux `/proc` file system. The relevant `syscr`/`syscw` counters are as of now
only documented with "_read I/O operations, i.e. syscalls like read() and
pread()_" and "_write I/O operations, i.e. syscalls like write() and pwrite()_".
Reference:
[Documentation/filesystems/proc.txt](https://github.com/torvalds/linux/blob/33920f1ec5bf47c5c0a1d2113989bdd9dfb3fae9/Documentation/filesystems/proc.txt#L1628)

Mean over the past sampling interval.

#### `proc_mem_rss_percent`

Fraction of process [resident set size](https://stackoverflow.com/a/21049737)
(RSS) relative to machine's physical memory size in `percent`.

Momentary state at sampling time.

#### `proc_mem_rss`, `proc_mem_vms`. `proc_mem_dirty`

Various memory usage metrics of the monitored process. See the [psutil
docs](https://psutil.readthedocs.io/en/release-5.3.0/#psutil.Process.memory_info)
for a quick summary of what the values mean. However, note that the values need
careful interpretation, as is hopefully obvious from discussions like
[this](https://serverfault.com/q/138427) and
[this](https://serverfault.com/q/138427).

Momentary snapshot at sampling time.


### Disk metrics

Only collected if Goeffel is invoked with the `--diskstats <DEV>` argument. The
resulting data column names contain the device name `<DEV>` (note however that
dashes in `<DEV>` get removed when building the column names).

Note that the conclusiveness of some of these disk metrics is limited. I believe
that [this blog post](https://blog.serverfault.com/2010/07/06/777852755/) nicely
covers a few basic Linux disk I/O concepts that should be known prior to reading
a meaning into these numbers.

#### `disk_<DEV>_util_percent`

This implements iostat's disk `%util` metric.

I like to think of it as the ratio between the actual (wall) time elapsed in the
sampled time interval, and the corresponding device's "busy time" in the very
same time interval, expressed in percent. The iostat documentation describes
this metric in the following words:

> Percentage of elapsed time during which I/O requests were issued to the device
> (bandwidth  utilization  for the device)."

This is the mean over the sampling interval.

**Note**: In case of modern storage systems `100 %` utilization usually does
**not** mean that the device is saturated. I would like to quote [Marc
Brooker](https://brooker.co.za/blog/2014/07/04/iostat-pct.html):

> As a measure of general IO busyness `%util` is fairly handy, but as an
> indication of how much the system is doing compared to what it can do, it's
> terrible.

#### `disk_<DEV>_write_latency_ms` and `disk_<DEV>_read_latency_ms`

This implements iostat's `w_await` which is documented with

> The average time (in milliseconds) for write requests issued to the device to
> be served. This includes the time spent by the requests in queue and the time
> spent servicing them.

On Linux this is built using `/proc/diskstats` documented
[here](https://github.com/torvalds/linux/blob/v5.3-rc3/Documentation/admin-guide/iostats.rst#io-statistics-fields).
Specifically, this uses field 8 ("number of milliseconds spent writing") and
field 5 ("number of writes completed"). Notably, the latter it is *not* the
merged write count but the user space write count (which seems to be what iostat
uses for calculating `w_await`).

This can be a useful metric, but please be aware of its meaning and limitations.
To put this into perspective, in an experiment I have seen that the following
can happen within a second of real time (observed via `iostat -x 1 | grep xvdh`
and via direct monitoring of `/proc/diskstats`): 3093 userspace write requests
served, merged into 22 device write requests, yielding a total of 120914
milliseconds "spent writing", resulting in a mean write latency of 25 ms. But
what do these 25 ms really mean here? On average, humans have less than two
legs, for sure. The current implementation method reproduces iostat output,
which was the initial goal. Suggestions for improvement are very welcome.

This is the mean over the sampling interval.

The same considerations hold true for `r_await`, correspondingly.

#### `disk_<DEV>_merged_read_rate_hz` and `disk_<DEV>_merged_write_rate_hz`

The _merged_ read and write request rate.

The Linux kernel attempts to merge individual user space requests before passing
them to the storage hardware. For non-random I/O patterns this greatly reduces
the rate of individual reads and writes issued to disk.

Built using fields 2 and 6 in `/proc/diskstats` documented
[here](https://github.com/torvalds/linux/blob/v5.3-rc3/Documentation/admin-guide/iostats.rst#io-statistics-fields).

This is the mean over the sampling interval.

#### `disk_<DEV>_userspace_read_rate_hz` and `disk_<DEV>_userspace_write_rate_hz`

The read and write request rate issued from user space point of view (before
merges).

Built using fields 1 and 5 in `/proc/diskstats` documented
[here](https://github.com/torvalds/linux/blob/v5.3-rc3/Documentation/admin-guide/iostats.rst#io-statistics-fields).

This is the mean over the sampling interval.


### System-wide metrics

`system_loadavg1`

`system_loadavg5`

`system_loadavg15`

`system_mem_available`

`system_mem_total`

`system_mem_used`

`system_mem_free`

`system_mem_shared`

`system_mem_buffers`

`system_mem_cached`

`system_mem_active`

`system_mem_inactive`


# Tips and tricks

## How to convert a Goeffel HDF5 file into a CSV file

I recommend to de-serialize and re-serialize using
[pandas](https://pandas.pydata.org/). Example one-liner:
```
python -c 'import sys; import pandas as pd; df = pd.read_hdf(sys.argv[1], key="goeffel_timeseries"); df.to_csv(sys.argv[2], index=False)' goeffel_20190718_213115.hdf5.0001 /tmp/hdf5-as-csv.csv
```
Note that this significantly inflates the file size (e.g., from 50 MiB to 300
MiB).

## How to visualize and browse the contents of an HDF5 file

At some point you might feel inclined to poke around in an HDF5 file created by
Goeffel or to do custom data inspection / processing. In that case I recommend
to use one of the various available open-source HDF5 tools for managing and
viewing HDF5 files. One GUI tool I have frequently used is
[ViTables](http://vitables.org/). Install it with `pip install vitables` and
then do e.g.

```text
vitables goeffel_20190718_213115.hdf5
```

This opens a GUI which allows for browsing the tabular time series data, for
viewing the meta data in the file, for exporting data as CSV, for querying the
data, and various other things.

## How to do quick data analysis using IPython and pandas

I recommend to start an [IPython](https://ipython.org/) REPL:
```text
pip install ipython  # if you have not done so yet
ipython
```
Load the HDF5 file into a `pandas` data frame:
```
In [1]: import pandas as pd
In [2]: df = pd.read_hdf('goeffel_timeseries__20190806_213704.hdf5', key='goeffel_timeseries')
```
From here you can do anything.

For example, let's have a look at the mean value of the actual sampling interval
used in this specific Goeffel time series:
```
In [3]: df['unixtime'].diff().mean()
Out[3]: 0.5003192476604296
```

Or, let's see how many threads the monitored process used at most during the
entire observation period:
```
In [4]: df['proc_num_threads'].max()
Out[4]: 1
```

## How to convert the `unixtime` column into a `pandas.DatetimeIndex`

The HDF5 file contains a `unixtime` column which contains canonical Unix
timestamp data ready to be consumed by a plethora of tools. If you are like me
and like to use `pandas` then it is good to know how to convert this into a
native `pandas.DateTimeIndex`:

```
In [1]: import pandas as pd
In [2]: df = pd.read_hdf('goeffel_timeseries__20190807_174333.hdf5', key='goeffel_timeseries')

# Now the data frame has an integer index.
In [3]: type(df.index)
Out[3]: pandas.core.indexes.numeric.Int64Index

# Parse unixtime column.
In [4]: timestamps = pd.to_datetime(df['unixtime'], unit='s')

# Replace the index of the data frame.
In [5]: df.index = timestamps

# Now the data frame has a DatetimeIndex.
In [6]: type(df.index)
Out[6]: pandas.core.indexes.datetimes.DatetimeIndex

# Let's look at some values.
In [7]: df.index[:5]
Out[7]:
DatetimeIndex(['2019-08-07 15:43:33.798929930',
               '2019-08-07 15:43:34.300590992',
               '2019-08-07 15:43:34.801260948',
               '2019-08-07 15:43:35.301798105',
               '2019-08-07 15:43:35.802226067'],
              dtype='datetime64[ns]', name='unixtime', freq=None)
```


# Valuable references

External references on the subject matter that I found useful during
development.

About system performance measurement, and kernel time bookkeeping:

- http://www.brendangregg.com/usemethod.html
- https://www.vividcortex.com/blog/monitoring-and-observability-with-use-and-red
- https://github.com/uber-common/cpustat/blob/master/README.md
- https://elinux.org/Kernel_Timer_Systems
- https://github.com/Leo-G/DevopsWiki/wiki/How-Linux-CPU-Usage-Time-and-Percentage-is-calculated

About disk I/O statistics:

- https://www.xaprb.com/blog/2010/01/09/how-linux-iostat-computes-its-results/
- https://www.kernel.org/doc/Documentation/iostats.txt
- https://blog.serverfault.com/2010/07/06/777852755/ (interpreting iostat output)
- https://unix.stackexchange.com/a/462732 (What are merged writes?)
- https://stackoverflow.com/a/8512978 (what is`%util` in iostat?)
- https://brooker.co.za/blog/2014/07/04/iostat-pct.html
- https://coderwall.com/p/utc42q/understanding-iostat
- https://www.percona.com/doc/percona-toolkit/LATEST/pt-diskstats.html

Others:

- https://serverfault.com/a/85481/121951 (about system memory statistics)

Musings about HDF5:

- https://cyrille.rossant.net/moving-away-hdf5/
- http://hdf-forum.184993.n3.nabble.com/File-corruption-and-hdf5-design-considerations-td4025305.html
- https://pytables-users.narkive.com/QH2WlyqN/corrupt-hdf5-files
- https://www.hdfgroup.org/2015/05/whats-coming-in-the-hdf5-1-10-0-release/
- https://stackoverflow.com/q/35837243/145400