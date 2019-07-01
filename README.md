# Messer

Measures the resource utilization of a specific process over time.

Built for Linux..

In addition to sampling process-specific data this program also measures the
utilization / saturation / error rate of system-wide resources making it
straightforward to put the process-specific metrics into context.

Highlights:

- High sampling rate.

- Can inspect a program subject to process ID changes. This is useful for
  longevity experiments when the program you want to monitor is expected to
  occasionally restart (for instance as of fail-over scenarios).

- Messer helps keeping the data organized: the time series data is written into
  an [HDF5](https://en.wikipedia.org/wiki/Hierarchical_Data_Format) file (
  annotate with relevant metadata such as program invocation time, system
  hostname, and Messer software version).

- Messer comes with a data plotting tool (separate from the data acquisition
  program).

Messer values measurement correctness very highly. Some aspects:

- It uses a sampling interval of 0.5 seconds for making narrow spikes visible.
  Note: the highest meaningful sampling rate is limited by the kernel's timer
  and bookkeeping system.

- The core sampling loop does little work besides the measurement itself.

- The measurement process which runs the core sampling loop writes each sample
  to a queue. Messer uses a separate process for consuming this queue and for
  emitting the time series data for later inspection (that is, the measurement
  is decoupled from persisting the data via an in-memory buffer, making the
  sampling rate more predictable upon disk write latency spikes, or generally
  upon backpressure).

## Motivation

This was born out of a need for solid tooling. We started with [pidstat from
sysstat](https://github.com/sysstat/sysstat/blob/master/pidstat.c), launched in
the following manner:

```
pidstat -hud -p $PID 1 1
```

We found that it does not properly account for multiple threads running in the
same process, and that various issues in that regard exist in this program
across various versions (see
[here](https://github.com/sysstat/sysstat/issues/73#issuecomment-349946051),
[here](https://github.com/sysstat/sysstat/commit/52977c479), and
[here](https://github.com/sysstat/sysstat/commit/a63e87996)).

The program [cpustat](https://github.com/uber-common/cpustat) open-sourced by
Uber has a delightful README about the general measurement methodology and
overall seems to be a great tool. However, it seems to be optimized for
interactive usage (whereas we were looking for a robust measurement program
which can be pointed at a process and then be left unattended for a significant
while) and there does not seem to be a decent approach towards persisting the
collected time series data on disk for later inspection (`cpustat` seems to be
able to write a binary file when using `-cpuprofile` but it is a little unclear
what this file contains and how to analyze the data).

The program [psrecord](https://github.com/astrofrog/psrecord) (which effectively
wraps [psutil](https://psutil.readthedocs.io/en/latest/)) has the same
fundamental idea as this code here; it however does not have a clear separation
of concerns between persisting the data to disk, performing the measurement
itself, and plotting the data, rendering it not production-ready for our
concerns.


### Measurands (columns, and their units)

The quantities intended to be measured.

#### `proc_io_read_throughput_mibps` and `proc_io_write_throughput_mibps`

Measures the disk I/O throughput of the inspected process, in `MiB/s`.

Based on Linux' `/proc/<pid>/io` `rchar` and `wchar`

> The number of bytes which this task has caused to be read from storage. This
> is simply the sum of bytes which this process passed to read() and pread().
> It includes things like tty IO and it is unaffected by whether or not actual
> physical disk IO was required (the read might have been satisfied from
> pagecache)

Note(JP): rename to `proc_disk_read_throughput_mibps`?

## Notes

- Messer does not asymmetrically hide measurement uncertainty. For example, you
  might see it measure a CPU utilization of a single-threaded process slightly
  larger than 100 %. That's simply the measurement error. In other tooling such
  as `sysstat` it seems to be common practice to _asymmetrically_ hide
  measurement uncertainty by capping values when they are known to in theory not
  exceed a certain threshold
  ([example](https://github.com/sysstat/sysstat/commit/52977c479d3de1cb2535f896273d518326c26722)).
