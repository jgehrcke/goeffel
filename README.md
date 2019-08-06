# Goeffel

Measures the resource utilization of a specific process over time.

This program also measures the utilization / saturation of system-wide resources
making it straightforward to put the process-specific metrics into context.

Built for Linux. Windows and Mac OS support might come.

**Highlights**:

- High sampling rate: by default, Goeffel uses a sampling interval of 0.5 seconds
  for making narrow spikes visible.
- Goeffel is built for monitoring a program subject to process ID changes. This
  is useful for longevity experiments when the monitored process occasionaly
  restarts (for instance as of fail-over scenarios).
- Goeffel can run unsupervised and infinitely long with predictable disk space
  requirements (it applies an output file rotation and retention policy).
- Goeffel helps keeping data organized: time series data is written into
  [HDF5](https://en.wikipedia.org/wiki/Hierarchical_Data_Format) files, and
  annotated with relevant metadata such as the program invocation time, system
  hostname, and Goeffel software version.
- Goeffel comes with a data plotting tool (separate from the data acquisition
  program).
- Goeffel values measurement correctness very highly. The core sampling loop does
  little work besides the measurement itself: it writes each sample to a queue.
  A separate process consumes this queue and persists the time series data to
  disk, for later inspection. This keeps the sampling rate predictable upon disk
  write latency spikes, or generally upon backpressure. This matters especially
  in cloud environments where we sometimes see fsync latencies of multiple
  seconds.


## Motivation

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
while) and there does not seem to be a decent approach towards persisting the
collected time series data on disk for later inspection (it seems to be able to
write a binary file when using `-cpuprofile` but it is a little unclear what
this file contains and how to analyze the data).

The program [psrecord](https://github.com/astrofrog/psrecord) (which effectively
wraps [psutil](https://psutil.readthedocs.io/en/latest/)) has a similar
fundamental idea as Goeffel; it however does not have a clear separation of
concerns between persisting the data to disk, performing the measurement itself,
and plotting the data, making it too error-prone and not production-ready.


## Usage

### Hints and tricks

#### Convert an HDF5 file to a CSV file

I recommend de-serialize and re-serialize using
[pandas](https://pandas.pydata.org/). Example one-liner:
```
python -c 'import sys; import pandas as pd; df = pd.read_hdf(sys.argv[1], key="goeffel_timeseries"); df.to_csv(sys.argv[2], index=False)' messer_20190718_213115.hdf5.0001 /tmp/hdf5-as-csv.csv
```
Note that this significantly inflates the file size (e.g., from 50 MiB to 300
MiB).


## Notes

- Goeffel tries to not asymmetrically hide measurement uncertainty. For example,
  you might see it measure a CPU utilization of a single-threaded process
  slightly larger than 100 %. That's simply the measurement error. In other
  tooling such as `sysstat` it seems to be common practice to _asymmetrically_
  hide measurement uncertainty by capping values when they are known to in
  theory not exceed a certain threshold
  ([example](https://github.com/sysstat/sysstat/commit/52977c479d3de1cb2535f896273d518326c26722)).

- Must be run with `root` privileges.

- The value `-1` has a special meaning for some metrics
  ([NaN](https://en.wikipedia.org/wiki/NaN), which cannot be represented
  properly in HDF5). Example: A disk write latency of `-1 ms` means that no
  write happened in the corresponding time interval.

- The highest meaningful sampling rate is limited by the kernel's timer and
  bookkeeping system.


## Measurands (columns, and their units)

The quantities intended to be measured.


#### `proc_cpu_id`

The ID of the CPU that this process is currently running on.

Momentary state at sampling time.


#### `proc_cpu_util_percent_total`

The CPU utilization of the process in `percent`.

Mean over the past sampling interval.

If the inspected process is known to contain just a single thread then
this can still sometimes be larger than 100 % as of measurement errors. If the
process contains more than one thread then this can go far beyond 100 %.

This is based on the sum of the time spent in user space and in kernel space.
For a more fine-grained picture the following two metrics are also available:
`proc_cpu_util_percent_user`, and `proc_cpu_util_percent_system`.


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

Based on Linux' `/proc/<pid>/io` `rchar` and `wchar`. A highly relevant
[piece of documentation](https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/Documentation/filesystems/proc.txt) (emphasis mine):

> The number of bytes which this task has caused to be read from storage. This
> is simply the sum of bytes which this process passed to read() and pread().
> *It includes things like tty IO* and it is unaffected by whether or not actual
> physical disk IO was required (*the read might have been satisfied from
> pagecache*)

Mean over the past sampling interval.


#### `proc_mem_rss_percent`

Fraction of process [resident set size](https://stackoverflow.com/a/21049737)
(RSS) relative to machine's physical memory size in `percent`.

Momentary state at sampling time.


#### `proc_ctx_switch_rate_hz`

The rate of ([voluntary and
involuntary](https://unix.stackexchange.com/a/442991)) context switches in `Hz`.

Mean over the past sampling interval.


(list incomplete)


## Valuable references

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