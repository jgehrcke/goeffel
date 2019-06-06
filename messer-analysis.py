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
import logging
import math
import re
import sys
import textwrap

from collections import Counter, OrderedDict
from datetime import datetime, timedelta

logfmt = "%(asctime)s.%(msecs)03d %(name)s %(levelname)s: %(message)s"
datefmt = "%y%m%d-%H:%M:%S"
logging.basicConfig(format=logfmt, datefmt=datefmt, level=logging.INFO)
log = logging.getLogger()


ARGS = None


def main():

    description = textwrap.dedent(
    """
    Process and plot one or multiple time series created with messer.py.
    """)

    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    subparsers = parser.add_subparsers()

    isparser = subparsers.add_parser('inspect', help='Inspect data file')
    isparser.add_argument(
        'inspect_inputfile',
        metavar='PATH',
        help='Path to data file',
    )

    plotparser = subparsers.add_parser('plot', help='Plot data')


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
        help='Every column is displayed in its own figure, potentially multiple sub plots from various columns'
    )

    plotparser.add_argument(
        '--subtitle',
        default='Default subtitle -- measured with messer.py',
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

    # TODO(JP): build this functionality out: print properly, integrate
    # properly with argparse.
    if hasattr(ARGS, 'inspect_inputfile'):
        import tables
        hdf5file = tables.open_file(ARGS.inspect_inputfile, 'r')
        print(f'File information:\n{hdf5file}')
        # TODO(JP): handle case when table does not exist.
        table = hdf5file.root.messer_timeseries
        print(
            f'Table information:\n'
            f'  Invocation time (local): {table.attrs.invocation_time_local}\n'
            f'  System hostname: {table.attrs.system_hostname}\n'
            f'  PID command: {table.attrs.messer_pid_command}\n'
            #f'  PID: {table.attrs.messer_pid}\n'
            f'  Messer schema version: {table.attrs.messer_schema_version}\n'
            f'  Number of rows: {table.nrows}\n'
        )
        print(f"Last row's (local) time: {table[-1]['isotime_local'].decode('ascii')}")
        print('Column names:\n  %s' % ('\n  '.join(c for c in table.colnames)))
        hdf5file.close()
        sys.exit(0)

    lazy_load_big_packages()

    dataframe_label_pairs = []
    for filepath, series_label in ARGS.series:
        dataframe_label_pairs.append(
            (parse_datafile_into_dataframe(filepath), series_label)
        )

    # Translate each `--column ....` argument into a dictionary.
    column_dicts = []
    keys = ('column_name', 'y_label', 'plot_title', 'rolling_wdw_width_seconds')
    for values in ARGS.column:
        # TODO: add check that rolling_wdw_width_seconds is an integer.
        column_dicts.append(dict(zip(keys, values)))

    for column_dict in column_dicts:
        plot_column_multiple_subplots(dataframe_label_pairs, column_dict)

    plt.show()


def lazy_load_big_packages():
    global np, pd, mpl, plt
    import numpy as np
    import pandas as pd
    import matplotlib as mpl
    import matplotlib.pyplot as plt
    plt.style.use('ggplot')


def plot_column_multiple_subplots(dataframe_label_pairs, column_dict):

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
            df[column_dict['column_name']].max() for df, _ in \
            dataframe_label_pairs
        )

        minval_across_series = min(
            df[column_dict['column_name']].min() for df, _ in \
            dataframe_label_pairs
        )

        diff = maxval_across_series - minval_across_series
        common_y_limit = (
            minval_across_series - 0.09 * diff,
            maxval_across_series + 0.09 * diff
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

        if common_y_limit is not None:
            plotsettings['ylim'] = common_y_limit

        plot_subplot(axs[idx-1], column_dict, series, plotsettings)

    # Align the subplots a little nicer, make more use of space. `hspace`: The
    # amount of height reserved for space between subplots, expressed as a
    # fraction of the average axis height
    plt.subplots_adjust(
        hspace=0.05, left=0.05, right=0.97, bottom=0.1, top=0.95)
    #plt.tight_layout()
    savefig(column_dict['plot_title'])


def plot_subplot(ax, column_dict, series, plotsettings):

    log.info('Plot column %s from %s', column_dict, series.name)

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
    window_width_seconds = int(column_dict['rolling_wdw_width_seconds'])
    if window_width_seconds != 0:

        # The raw samples are insightful, especially for seeing the outliers in
        # the distribution. However, also plot a rolling window average. It
        # shows where the distribution has its weight.
        log.info('Perform rolling window analysis')
        rollingwindow = series.rolling(
            window='%ss' % window_width_seconds
        )

        #rolling_window_mean = rollingwindow.sum() / float(window_width_seconds)
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
            #linestyle='solid',
            linestyle='None',
            color='black',
            marker='.',
            markersize=1,
            #markeredgecolor='gray'
            )

    # With `subplots()` sharex option this can be set for all subplots.
    ax.set_xlabel('Time (UTC)', fontsize=12)

    if plotsettings['show_y_label']:
        # If no custom y label was provided fall back to using series name.
        ax.set_ylabel(
            column_dict['y_label'] if column_dict['y_label'] else series.name,
            fontsize=12
        )

    # The legend story is shitty with pandas intertwined w/ mpl.
    # http://stackoverflow.com/a/30666612/145400
    if plotsettings['show_legend']:
        legend = ['raw samples']
        if window_width_seconds != 0:
            legend.append('%s s rolling window average' % window_width_seconds)
        ax.legend(
            legend,
            numpoints=4,
            loc=ARGS.legend_loc if ARGS.legend_loc else 'best'
        )

    if 'ylim' in plotsettings:

        ylim = plotsettings['ylim']

        # Divide limits by norm factor if set.
        nf = ARGS.normalization_factor
        if nf != 0:
            ylim = ylim[0] / nf, ylim[1] / nf

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


def parse_datafile_into_dataframe(datafilepath):

    log.info('Read data from: %s', datafilepath)

    try:
        df = pd.read_csv(
            datafilepath,
            comment='#',
            index_col=0
        )
    except ValueError as e:
        log.debug('Falling back to HDF parsing, CSV parsing failed: %s', e)
        df = pd.read_hdf(datafilepath, key='messer_timeseries')

    # Parse Unix timestamps into a DateTimeIndex object and replace the
    # dataframe's index (integers) with the new one.
    timestamps = pd.to_datetime(df['unixtime'], unit='s')
    df.index = timestamps
    log.info('Number of samples: %s', len(df))

    log.info('Check monotonicity of time index')
    for i, t in enumerate(timestamps[1:]):
        t_previous = timestamps[i]
        if t < t_previous:
            delta = t_previous - t
            log.warning(
                'log not monotonic at sample number %s (delta: %s)',
                i,
                delta
            )

    starttime = timestamps.iloc[0]
    log.info('Starttime (UTC): %s', starttime)
    timespan = timestamps.iloc[-1] - starttime
    log.info('Time span: %r', pretty_timedelta(timespan))

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

    fpath_figure = fname + '.png'
    log.info('Writing PNG figure to %s', fpath_figure)
    plt.savefig(fpath_figure, dpi=150)


def pretty_timedelta(timedelta):
    seconds = int(timedelta.total_seconds())
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    if days > 0:
        return '%dd%dh%dm%ds' % (days, hours, minutes, seconds)
    elif hours > 0:
        return '%dh%dm%ds' % (hours, minutes, seconds)
    elif minutes > 0:
        return '%dm%ds' % (minutes, seconds)
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
