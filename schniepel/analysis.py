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

import argparse
import copy
import logging
import os
import math
import re
import sys

from datetime import datetime


logfmt = "%(asctime)s.%(msecs)03d %(levelname)s: %(message)s"
datefmt = "%y%m%d-%H:%M:%S"
logging.basicConfig(format=logfmt, datefmt=datefmt, level=logging.DEBUG)
log = logging.getLogger()
logging.getLogger('matplotlib').setLevel('INFO')


COLUMN_PLOT_CONFIGS = {
    'proc_cpu_util_percent_total': {
        'y_label': 'Process CPU util (total) [%]',
        'plot_title': 'foo',
        'rolling_wdw_width_seconds': 5,
    },
    'proc_disk_read_rate_hz': {
        'y_label': 'Process read() rate [Hz]',
        'plot_title': 'foo',
        'rolling_wdw_width_seconds': 5,
        'yscale': 'symlog'
    },
    'proc_disk_write_rate_hz': {
        'y_label': 'Process write() rate [Hz]',
        'plot_title': 'foo',
        'rolling_wdw_width_seconds': 5,
        'yscale': 'symlog'
    },
    'proc_disk_write_throughput_mibps': {
        'y_label': 'Process write() tp [MiB/s]',
        'plot_title': 'foo',
        'rolling_wdw_width_seconds': 5,
        'yscale': 'symlog'
    },
    'system_loadavg1': {
        'y_label': 'System 1 min load avg',
        'plot_title': 'foo',
        'rolling_wdw_width_seconds': 0
    },
    'disk_DEVNAME_util_percent': {
        'y_label': 'DEVNAME util [%]',
        'plot_title': 'foo',
        'rolling_wdw_width_seconds': 5
    },
    'disk_DEVNAME_write_latency_ms': {
         'y_label': 'DEVNAME wl [ms]',
         'plot_title': 'foo',
         'rolling_wdw_width_seconds': 5
    },
}


# Populated by `parse_cmdline_args()`.
ARGS = None


def main():

    # This modifies the global `ARGS`.
    parse_cmdline_args()

    if hasattr(ARGS, 'inspect_inputfile'):
        inspect_data_file()
        sys.exit(0)

    # Importing matplotlib is slow. Defer until it known that it is needed.
    log.debug('Import big packages')
    lazy_load_big_packages()

    if ARGS.command == 'magic':
        cmd_magic()
        sys.exit(0)

    raise NotImplementedError

    # What follows is super useful functionality but this should be properly
    # abstracted in its own subcommand.

    # dataframe_label_pairs = []
    # for filepath, series_label in ARGS.series:
    #     dataframe_label_pairs.append(
    #         (parse_datafile_into_dataframe(filepath), series_label)
    #     )

    # # Translate each `--column ....` argument into a dictionary.
    # column_dicts = []
    # keys = ('column_name', 'y_label', 'plot_title', 'rolling_wdw_width_seconds')
    # for values in ARGS.column:
    #     # TODO: add check that rolling_wdw_width_seconds is an integer.
    #     column_dicts.append(dict(zip(keys, values)))

    # for column_dict in column_dicts:
    #     plot_column_multiple_subplots(dataframe_label_pairs, column_dict)

    # plt.show()


def parse_cmdline_args():

    description = 'Process and plot one or multiple time series created with Schniepel'

    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    subparsers = parser.add_subparsers(dest='command')
    # From Python 3.7 on `required=True` can be provided as kwarg above. This
    # means that one of the commands is required, and a proper error message is
    # generated if no sub command is provided.
    subparsers.required = True

    isparser = subparsers.add_parser('inspect', help='Inspect data file')
    isparser.add_argument(
        'inspect_inputfile',
        metavar='PATH',
        help='Path to Schniepel data file.',
    )

    magicparser = subparsers.add_parser('magic', help='Magic ;]')
    magicparser.add_argument(
        'datafile_for_magicplot',
        metavar='PATH',
        help='Schniepel data file containing process and system metrics.'
    )
    # Allow only _one_ of the following four options.
    meg = magicparser.add_mutually_exclusive_group()
    meg.add_argument(
        '--first',
        metavar='TIME OFFSET STRING',
        help=(
            'Analyze the first part of the time series data. '
            'Use pandas time offset string (such as 2D, meaning two days).'
        )
    )
    meg.add_argument(
        '--last',
        metavar='TIME OFFSET STRING',
        help=(
            'Analyze the list part of the time series data. Use pandas '
            'time offset string (such as 2D, meaning two days).'
        )
    )
    meg.add_argument(
       '--head',
       metavar='N',
       type=int,
       help='Analyze only the first N rows of the data table.'
    )
    meg.add_argument(
       '--tail',
       metavar='N',
       type=int,
       help='Analyze only the last N rows of the data table.'
    )

    magicparser.add_argument(
        '--metric',
        metavar='METRIC_NAME',
        action='append'
    )

    magicparser.add_argument(
        '--interactive-plot',
        action='store_true'
    )

    plotparser = subparsers.add_parser('plot', help='Plot data in a flexible manner')
    plotparser.add_argument(
        '--series',
        nargs=2,
        metavar=('DATAFILE_PATH', 'DATASET_LABEL'),
        action='append',
        required=True,
        help='Data file containing one or multiple time series (column(s))'
    )

    plotparser.add_argument(
        '--column',
        nargs=4,
        metavar=('COLUMN_NAME', 'Y_LABEL', 'PLOT_TITLE', 'ROLLING_WINDOW_WIDTH_SECONDS'),
        action='append',
        required=True,
        help=(
            'Every column is displayed in its own figure, potentially '
            'multiple sub plots from various columns.'
        )
    )

    plotparser.add_argument(
        '--subtitle',
        default='Default subtitle -- measured with Schniepel',
        help='Set plot subtitle'
    )
    plotparser.add_argument('--samescale', action='store_true', default=True)
    plotparser.add_argument('--legend-loc')
    plotparser.add_argument('--show-legend-in-plot', default=1, type=int)
    plotparser.add_argument(
        '--normalization-factor',
        default=0,
        type=float,
        help='All values are divided by this number.'
    )
    plotparser.add_argument(
        '--custom-y-limit',
        nargs=2,
        type=float,
        metavar=('YLIM_MIN', 'YLIM_MAX'),
        help='Set a custom global y limit (min, max) to all plots'
    )
    global ARGS
    ARGS = parser.parse_args()


def inspect_data_file():
    if not os.path.isfile(ARGS.inspect_inputfile):
        sys.exit('Not a file: %s' % (ARGS.inspect_inputfile, ))

    import tables

    with tables.open_file(ARGS.inspect_inputfile, 'r') as hdf5file:
        try:
            table = hdf5file.root.schniepel_timeseries
        except tables.exceptions.NoSuchNodeError:
            log.info('HDF5 file details:\n%s', hdf5file)
            log.error('Could not find /schniepel_timeseries object. Exit.')
            sys.exit(1)

        print(
            f'Measurement meta data:\n'
            f'  System hostname: {table.attrs.system_hostname}\n'
            f'  Invocation time (local): {table.attrs.invocation_time_local}\n'
            f'  PID command: {table.attrs.schniepel_pid_command}\n'
            f'  PID: {table.attrs.schniepel_pid}\n'
            f'  Sampling interval: {table.attrs.schniepel_sampling_interval_seconds} s\n'
            # f'  Schniepel schema version: {table.attrs.schniepel_schema_version}\n'
        )

        frlt = table[0]['isotime_local'].decode('ascii')
        lrlt = table[-1]['isotime_local'].decode('ascii')
        npoints = table.nrows * len(table.colnames)

        print(f'Table properties:')
        print(f'  Number of rows: {table.nrows}')
        print(f'  Number of columns: {len(table.colnames)}')
        print(f'  Number of data points (rows*columns): {npoints:.2E}')
        print(f"  First row's (local) time: {frlt}")
        print(f"  Last  row's (local) time: {lrlt}")
        print('  Time span: %s' % (pretty_timedelta(
            datetime.fromtimestamp(table[-1]['unixtime']) -
            datetime.fromtimestamp(table[0]['unixtime'])
        ), ))

        print('\nColumn names:\n  %s' % ('\n  '.join(c for c in table.colnames)))


def lazy_load_big_packages():
    global np, pd, mpl, plt
    import numpy as np
    import pandas as pd
    import matplotlib as mpl
    import matplotlib.pyplot as plt
    plt.style.use('ggplot')


def cmd_magic():
    # Note(JP): using --tail / --head / --first / --last can also be used to
    # speed up parsing.
    # dataframe = parse_hdf5file_into_dataframe(ARGS.datafile_for_magicplot)

    # https://stackoverflow.com/questions/46493567/how-to-read-nrows-from-pandas-hdf-storage
    # https://github.com/pandas-dev/pandas/issues/11188
    # and this duplicate: https://github.com/pandas-dev/pandas/issues/14568
    #
    # and my solution attempt: https://github.com/pandas-dev/pandas/pull/26818
    #

    def get_table_metadata():
        import tables
        # Rely on this being a valid HDF5 file.
        with tables.open_file(ARGS.datafile_for_magicplot, 'r') as hdf5file:
            table = hdf5file.root.schniepel_timeseries
            table_metadata = copy.copy(table.attrs)

        return table_metadata

    dataframe = parse_hdf5file_into_dataframe(
        ARGS.datafile_for_magicplot,
        # startrow=ARGS.tail,
        # stoprow=ARGS.head,
        first=ARGS.first,
        last=ARGS.last
    )

    metadata = get_table_metadata()

    fig, custom_tight_layout_func = plot_magic(dataframe, metadata)

    # Apply custom "tight layout" routine after resize. This is when the user
    # actually resizes the figure window or, more importantly, upon first draw
    # where the window might be resized to screen dimensions. In some cases this
    # is required for the initial figure to be displayed nicely. In the future
    # the returned callback ID could be used to disconnect, i.e. to make this a
    # one-time operation (if that turns out to be required).
    _ = fig.canvas.mpl_connect('resize_event', custom_tight_layout_func)

    if ARGS.interactive_plot:
        plt.show()


def plot_magic(dataframe, metadata):
    """
    Create a single figure with multiple subplots. Each subplot comes from a
    different column in the same dataframe.
    """

    columns_to_plot = [
        'proc_cpu_util_percent_total',
        'proc_disk_read_rate_hz',
        'proc_disk_write_rate_hz',
        'proc_disk_write_throughput_mibps',
        'system_loadavg1',
    ]

    additional_metrics = list(ARGS.metric)
    for m in additional_metrics:
        columns_to_plot.append(m)

    # Note(JP): this is a quick workaround to populate properties required in
    # code path downstream.
    ARGS.normalization_factor = 0
    ARGS.legend_loc = None
    ARGS.custom_y_limit = None

    column_count = len(columns_to_plot)

    # Create a new figure.
    plt.figure()

    # Defaults are 6.4x4.8 inches at 100 dpi, make canvas significantly larger
    # so that more details can be shown. But... vertically! :)
    # Make vertical size dependent on column count.
    # Note: `show()` adjusts the figure size to the screen size. Which is
    # undesired here. This is basically the same problem as discussed in
    # https://github.com/matplotlib/matplotlib/issues/7338 -- scroll bars
    # would be an appropriate solution. Interesting:
    # https://stackoverflow.com/a/42624276
    figure_height_inches = 2.28 * column_count

    fig = plt.gcf()
    fig.set_size_inches(12, figure_height_inches)

    fig.text(
        0.5, 0.985,
        f'Schniepel time series ({metadata.invocation_time_local})',
        verticalalignment='center',
        horizontalalignment='center',
        fontsize=13
    )

    fig.text(
        0.5, 0.970,
        f'hostname: {metadata.system_hostname}, PID command: {metadata.schniepel_pid_command}',
        verticalalignment='center',
        horizontalalignment='center',
        fontsize=10,
        color='gray'
    )

    # Subplot structure: one column, and as many rows as data columns. Create a
    # set of (empty) subplots at once; with a shared x axis (x tick labels are
    # hidden except at the bottom). From the "Creating adjacent subplots" demo
    # in the mpl docs. `subplots()` returns a list of Axes objects. Each Axes
    # object can later be `.plot()`ted on.

    axs = fig.subplots(column_count, 1, sharex=True)

    if column_count == 1:
        axs = [axs]

    # common_y_limit = None
    # if ARGS.samescale:

    #     maxval_across_series = max(
    #         df[column_dict['column_name']].max() for df, _ in \
    #         dataframe_label_pairs
    #     )

    #     minval_across_series = min(
    #         df[column_dict['column_name']].min() for df, _ in \
    #         dataframe_label_pairs
    #     )

    #     diff = maxval_across_series - minval_across_series
    #     common_y_limit = (
    #         minval_across_series - 0.09 * diff,
    #         maxval_across_series + 0.09 * diff
    #     )

    # Note(JP): with the `sharex` behavior above it seems like the order of
    # subplots created determines the xlimits set for _all_ subplots. That is,
    # for example, if the last subplot shows a time series that is shorter than
    # the previous subplots then this takes precedence and the other time series
    # are shown only partially. Make sure that all data is shown! Find smallest
    # and biggest timestamps across all series and use those values as x limit,
    # for all plots, making sure that all data is shown.
    mintime_across_series = dataframe.index[0]
    maxtime_across_series = dataframe.index[-1]
    diff = maxtime_across_series - mintime_across_series
    common_x_limit = (
        mintime_across_series - 0.03 * diff,
        maxtime_across_series + 0.03 * diff
    )

    def _get_column_plot_config_for_colname(colname):

        # Special treatment for disk metrics: Extract disk devname from metric
        # name. Then get generic disk-related column plot settings, but inject
        # the specific disk devname into text (such as labels).

        if colname.startswith('disk_'):
            m = re.match('disk_(?P<devname>.*?)_.*', colname)
            disk_devname = m.group('devname')
            # Build generic column name from specific column name
            # so that the subsequent dict lookup succeeds.
            colname = colname.replace(disk_devname, 'DEVNAME')

        # Create copy because of the disk-related modification below.
        column_plot_config = COLUMN_PLOT_CONFIGS[colname].copy()

        # Now, when these are disk metric plot settings then do some text
        # processing: insert specific disk device name.
        if colname.startswith('disk_'):
            for k, v in column_plot_config.items():
                if isinstance(v, str):
                    log.info('replace')
                    newvalue = v.replace('DEVNAME', disk_devname)
                    column_plot_config[k] = newvalue

        return column_plot_config

    # Plot individual subplots.
    for idx, colname in enumerate(columns_to_plot, 1):
        series = dataframe[colname]

        # Column-specific plot config such as y label, largely depends on the
        # metric itself.
        column_plot_config = _get_column_plot_config_for_colname(colname)

        # Subplot-specific plot config, independent of the metric, mainly
        # dependent on the position of the subplot.
        subplotsettings = {}
        subplotsettings['show_y_label'] = True

        # Plot y axis label only at central subplot.
        # plotsettings['show_y_label'] = \
        #    True if idx == math.ceil(dataframe_count/2) else False

        # Show legend only in first row (by default, can be modified)
        # plotsettings['show_legend'] = True #  if idx == ARGS.show_legend_in_plot else False
        subplotsettings['show_legend'] = True if idx == 1 else False
        subplotsettings['series_label'] = ''

        subplotsettings['xlim'] = common_x_limit

        # if common_y_limit is not None:
        #    plotsettings['ylim'] = common_y_limit

        plot_subplot(axs[idx-1], column_plot_config, series, subplotsettings)

    # Align the subplots a little nicer, make more use of space. `hspace`: The
    # amount of height reserved for space between subplots, expressed as a
    # fraction of the average axis height.

    # Note that `tight_layout` does not consider `fig.suptitle()`. Also see
    # https://stackoverflow.com/a/45161551/145400
    # plt.subplots_adjust(
    #    hspace=0.05 left=0.05, right=0.97, bottom=0.1, top=0.95)
    # plt.tight_layout()

    # Note that subplots_adjust must be called after any calls to tight_layout,
    # or there will be no effect of calling subplots_adjust. Also see
    # https://stackoverflow.com/a/8248506/145400

    def custom_tight_layout_func(event=None):
        """This function can be called as a callback in response to matplotlib
        events such as a window resize event.
        """
        plt.tight_layout(rect=[0, 0, 1, 0.97])
        # hspace controls the _vertical_ space between subplots.
        plt.subplots_adjust(hspace=0.019)

    custom_tight_layout_func()

    # plt.tight_layout()

    savefig(f'schniepel_magicplot_{metadata.system_hostname}_{metadata.invocation_time_local}')

    # Return matplotlib figure object for further processing for interactive
    # mode.
    return fig, custom_tight_layout_func


def plot_column_multiple_subplots(dataframe_label_pairs, column_dict):
    """
    Create a single figure with multiple subplots. There two modes:

    - each subplot comes from the same column in different dataframes (old)
    - each subplot comes from a different column in the same dataframe (new)
    """

    dataframe_count = len(dataframe_label_pairs)

    plt.figure()

    # Defaults are 6.4x4.8 inches at 100 dpi, make canvas significantly larger
    # so that more details can be shown.
    fig = plt.gcf()
    fig.set_size_inches(13, 9)

    # Add title and subtitle to figure.
    fig.text(
        0.5, 0.98,
        column_dict['plot_title'],
        verticalalignment='center',
        horizontalalignment='center',
        fontsize=14
    )

    fig.text(
        0.5, 0.96,
        ARGS.subtitle,
        verticalalignment='center',
        horizontalalignment='center',
        fontsize=10,
        color='gray'
    )

    # Subplot structure: one column, and as many rows as data files. Create a
    # set of (empty) subplots at once; with a shared x axis (x tick labels are
    # hidden except at the bottom). From the "Creating adjacent subplots" demo
    # in the mpl docs. `subplots()` returns a list of Axes objects. Each Axes
    # object can later be `.plot()`ted on.

    axs = fig.subplots(dataframe_count, 1, sharex=True)

    if dataframe_count == 1:
        axs = [axs]

    common_y_limit = None
    if ARGS.samescale:

        maxval_across_series = max(
            df[column_dict['column_name']].max() for df, _ in
            dataframe_label_pairs
        )

        minval_across_series = min(
            df[column_dict['column_name']].min() for df, _ in
            dataframe_label_pairs
        )

        diff = maxval_across_series - minval_across_series
        common_y_limit = (
            minval_across_series - 0.09 * diff,
            maxval_across_series + 0.09 * diff
        )

    # Note(JP): with the `sharex` behavior above it seems like the order of
    # subplots created determines the xlimits set for _all_ subplots. That is,
    # for example, if the last subplot shows a time series that is shorter than
    # the previous subplots then this takes precedence and the other time series
    # are shown only partially. Make sure that all data is shown! Find smallest
    # and biggest timestamps across all series and use those values as x limit,
    # for all plots, making sure that all data is shown.
    mintime_across_series = min(df.index[0] for df, _ in dataframe_label_pairs)
    maxtime_across_series = max(df.index[-1] for df, _ in dataframe_label_pairs)
    diff = maxtime_across_series - mintime_across_series
    common_x_limit = (
        mintime_across_series - 0.03 * diff,
        maxtime_across_series + 0.03 * diff
    )

    # Plot individual subplots.
    for idx, (dataframe, series_label) in enumerate(dataframe_label_pairs, 1):

        plotsettings = {}

        series = dataframe[column_dict['column_name']]

        # Plot y axis label only at central subplot.
        plotsettings['show_y_label'] = \
            True if idx == math.ceil(dataframe_count/2) else False

        # Show legend only in first row (by default, can be modified)
        plotsettings['show_legend'] = True if idx == ARGS.show_legend_in_plot else False
        plotsettings['series_label'] = series_label

        plotsettings['xlim'] = common_x_limit

        if common_y_limit is not None:
            plotsettings['ylim'] = common_y_limit

        plot_subplot(axs[idx-1], column_dict, series, plotsettings)

    # Align the subplots a little nicer, make more use of space. `hspace`: The
    # amount of height reserved for space between subplots, expressed as a
    # fraction of the average axis height
    plt.subplots_adjust(
        hspace=0.05, left=0.05, right=0.97, bottom=0.1, top=0.95)
    # plt.tight_layout()
    savefig(column_dict['plot_title'])


def plot_subplot(ax, column_plot_config, series, plotsettings):

    log.info('Plot column %s from %s', column_plot_config, series.name)

    # Set currently active axis to axis object handed over to this function.
    # That makes df.plot() add the data to said axis.
    plt.sca(ax)

    if ARGS.normalization_factor != 0:
        log.info('Apply normaliztion factor: %s', ARGS.normalization_factor)
        series = series / ARGS.normalization_factor

    ax = series.plot(
        linestyle='None',
        color='gray',
        marker='.',
        markersize=3,
        markeredgecolor='gray'
    )

    # Conditionally create a rolling window mean plot on top of the raw
    # samples.
    window_width_seconds = int(column_plot_config['rolling_wdw_width_seconds'])
    if window_width_seconds != 0:

        # The raw samples are insightful, especially for seeing the outliers in
        # the distribution. However, also plot a rolling window average. It
        # shows where the distribution has its weight.
        log.info('Perform rolling window analysis')
        rollingwindow = series.rolling(
            window='%ss' % window_width_seconds
        )

        # rolling_window_mean = rollingwindow.sum() / float(window_width_seconds)
        rolling_window_mean = rollingwindow.mean()

        # In the resulting Series object, the request rate value is assigned to
        # the right window boundary index value (i.e. to the newest timestamp in
        # the window). For presentation it is more convenient to have it
        # assigned (approximately) to the temporal center of the time window.
        # That makes sense for intuitive data interpretation of a single rolling
        # window time series, but is essential for meaningful presentation of
        # multiple rolling window series in the same plot (when their window
        # width varies). Invoking `rolling(..., center=True)` however yields
        # `NotImplementedError: center is not implemented for datetimelike and
        # offset based windows`. As a workaround, shift the data by half the
        # window size to 'the left': shift the timestamp index by a constant /
        # offset.
        offset = pd.DateOffset(seconds=window_width_seconds / 2.0)
        rolling_window_mean.index = rolling_window_mean.index - offset

        rolling_window_mean.plot(
            # linestyle='solid',
            linestyle='None',
            color='black',
            marker='.',
            markersize=1,
            # markeredgecolor='gray'
            )

    if 'yscale' in column_plot_config:
        if column_plot_config['yscale'] == 'symlog':
            if 'ylim' not in plotsettings:
                log.info('symlog: set lower ylim to 0')
                # Make sure to show the lower end, the zero, by default.
                _prevmax = ax.get_ylim()[1]
                ax.set_ylim((0, _prevmax * 1.4))
            # https://github.com/matplotlib/matplotlib/issues/7008
            # https://github.com/matplotlib/matplotlib/issues/10369
            ax.set_yscale(
                'symlog',
                linthreshy=1,
                linscaley=0.25,
                subsy=[2, 3, 4, 5, 6, 7, 8, 9]
            )
        else:
            ax.set_yscale(column_plot_config['yscale'])

    # With `subplots()` sharex option this can be set for all subplots.
    ax.set_xlabel('Time (UTC)', fontsize=10)

    if plotsettings['show_y_label']:
        # If no custom y label was provided fall back to using series name.
        ax.set_ylabel(
            column_plot_config['y_label'] if column_plot_config['y_label'] else series.name,
            fontsize=9
        )

    # The legend story is shitty with pandas intertwined w/ mpl.
    # http://stackoverflow.com/a/30666612/145400
    if plotsettings['show_legend']:
        legend = ['raw samples']
        if window_width_seconds != 0:
            legend.append('%s s rolling window mean' % window_width_seconds)
        ax.legend(
            legend,
            numpoints=4,
            loc=ARGS.legend_loc if ARGS.legend_loc else 'best'
        )

    ax.set_xlim(plotsettings['xlim'])

    # https://stackoverflow.com/a/11386056/145400
    ax.tick_params(axis='x', which='major', labelsize=8)
    ax.tick_params(axis='y', which='major', labelsize=8)

    if 'ylim' in plotsettings:

        ylim = plotsettings['ylim']

        # Divide limits by norm factor if set.
        nf = ARGS.normalization_factor
        if nf != 0:
            ylim = ylim[0] / nf, ylim[1] / nf

        log.info('set custom y lim')
        ax.set_ylim(ylim)

    # A custom Y limit takes precedence over the limit set above.
    if ARGS.custom_y_limit:
        ax.set_ylim(ARGS.custom_y_limit)

    # Add tiny series_label label in the top-left corner of the subplot.
    ax.text(
        0.01, 0.88,
        plotsettings['series_label'],
        verticalalignment='center',
        fontsize=8,
        transform=ax.transAxes
    )


def parse_hdf5file_into_dataframe(
        filepath, startrow=None, stoprow=None, first=None, last=None):

    # df = pd.read_csv(
    #     datafilepath,
    #     comment='#',
    #     index_col=0
    # )

    log.info('Read data from HDF5 file: %s', filepath)

    # Note(JP): the `start` and `stop` approach may speed up reading very large
    # HDF5 files, but requires https://github.com/pandas-dev/pandas/pull/26818/
    # to be addressed. Note that for a 60 MB (compressed) HDF5 file with 10 days
    # worth of time series data the parsing takes 2 seconds on my machine. That
    # is, using `start` and `stop` may only save about that much (1-2 seconds)
    # of processing time and a bit of memory. That is, this technique only
    # becomes meaningful for O(GB)-sized (compressed) HDF5 files.
    df = pd.read_hdf(
        filepath,
        key='schniepel_timeseries',
        # start=startrow,
        # stop=stoprow,
    )

    # Parse Unix timestamps into a `pandas.DateTimeIndex` object and replace the
    # DataFrame's index (integers) with the new one.
    log.info("Convert `unixtime` column to pandas' timestamps")
    timestamps = pd.to_datetime(df['unixtime'], unit='s')
    df.index = timestamps

    log.info('Number of samples (rows): %s', len(df))

    log.info('Check monotonicity of time index')
    if not df.index.is_monotonic_increasing:
        log.warning('Index not monotonic. Looking deeper.')
        # Note(JP): can probably be built faster.
        for i, t in enumerate(timestamps[1:]):
            t_previous = timestamps[i]
            if t < t_previous:
                delta = t_previous - t
                log.warning(
                    'log not monotonic at sample number %s (delta: %s)',
                    i,
                    delta
                )

    if first:
        log.info('Analyze only the first part of time series, offset: %s', first)
        df = df.first(first)
        assert not last, 'both must not be provided'

    if last:
        log.info('Analyze only the last part of time series, offset: %s', last)
        df = df.last(last)
        assert not first, 'both must not be provided'

    starttime = df.index[0]
    log.info('Time series start time (UTC): %s', starttime)
    timespan = df.index[-1] - starttime
    log.info('Time series time span: %r', pretty_timedelta(timespan))

    return df


def savefig(title):
    today = datetime.now().strftime('%Y-%m-%d')

    # Lowercase, replace special chars with whitespace, join on whitespace.
    cleantitle = '-'.join(re.sub('[^a-z0-9]+', ' ', title.lower()).split())

    fname = today + '_' + cleantitle
    fpath_cmd = fname + '.command'

    log.info('Writing command to %s', fpath_cmd)
    command = poor_mans_cmdline()
    with open(fpath_cmd, 'w') as f:
        f.write(command)

    log.info('Writing figure as PNG to %s', fname + '.png')
    plt.savefig(fname + '.png', dpi=200)

    log.info('Writing figure as PDF to %s', fname + '.pdf')
    plt.savefig(fname + '.pdf')


def pretty_timedelta(timedelta):
    seconds = int(timedelta.total_seconds())
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if days > 0:
        return '%dd %dh %dm %ds' % (days, hours, minutes, seconds)
    elif hours > 0:
        return '%dh %dm %ds' % (hours, minutes, seconds)
    elif minutes > 0:
        return '%dm %ds' % (minutes, seconds)
    else:
        return '%ds' % (seconds,)


def poor_mans_cmdline():
    command_fragments = []
    command_fragments.append("python " + sys.argv[0])

    for arg in sys.argv[1:]:
        if arg.startswith('--'):
            command_fragments.append(' \\\n')
            command_fragments.append(arg)
        else:
            if " " in arg:
                command_fragments.append(" '%s'" % (arg, ))
            else:
                command_fragments.append(" %s" % (arg, ))

    command_fragments.append('\n')

    return ''.join(command_fragments)


if __name__ == "__main__":
    main()
