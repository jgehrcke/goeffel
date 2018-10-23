#!/usr/bin/env python
# Copyright 2018 Jan-Philip Gehrcke
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import argparse
import logging
import math
import re
import sys
import textwrap

from collections import Counter, OrderedDict
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
plt.style.use('ggplot')


logfmt = "%(asctime)s.%(msecs)03d %(name)s %(levelname)s: %(message)s"
datefmt = "%y%m%d-%H:%M:%S"
logging.basicConfig(format=logfmt, datefmt=datefmt, level=logging.INFO)
log = logging.getLogger()

ARGS = None

def main():

    description = textwrap.dedent(
    """
    Process and plot one or multiple pidstat time series created via periodic
    invocation of

        pidstat -hud -p $PID 1 1 | tail -n2 &>> datafile
    """)

    parser = argparse.ArgumentParser(
        description=description,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        '--series',
        nargs=2,
        metavar=('DATAFILE_PATH', 'DATASET_LABEL'),
        action='append',
        required=True,
        help='Data file containing one or multiple time series (column(s))'
    )

    parser.add_argument(
        '--column',
        nargs=4,
        metavar=('COLUMN_NAME', 'Y_LABEL', 'PLOT_TITLE', 'ROLLING_WINDOW_WIDTH_SECONDS'),
        action='append',
        required=True
    )

    parser.add_argument('--subtitle', required=True)
    parser.add_argument('--samescale', action='store_true', default=True)
    global ARGS
    ARGS = parser.parse_args()

    dataframe_label_pairs = []
    for filepath, series_label in ARGS.series:
        dataframe_label_pairs.append(
            (parse_datafile_into_dataframe(filepath), series_label)
        )

    # Translate each `--column ....` argument into a dictionary.
    column_dicts = []
    keys = ('column_name', 'y_label', 'plot_title', 'rolling_wdw_width_seconds')
    for values in ARGS.column:
        # Add check that rolling_wdw_width_seconds is an integer.
        column_dicts.append(dict(zip(keys, values)))

    for column_dict in column_dicts:
        plot_column_multiple_subplots(dataframe_label_pairs, column_dict)

    plt.show()


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
    for idx0, (dataframe, series_label) in enumerate(dataframe_label_pairs):
        idx1 = idx0 + 1

        plotsettings = {}

        series = dataframe[column_dict['column_name']]

        # Plot y axis label only at central subplot.
        plotsettings['show_y_label'] = \
            True if idx1 == math.ceil(dataframe_count/2) else False

        # Show legend only in first row.
        plotsettings['show_legend'] = True if idx1 == 1 else False
        plotsettings['series_label'] = series_label

        if common_y_limit is not None:
            plotsettings['ylim'] = common_y_limit

        plot_subplot(axs[idx0], column_dict, series, plotsettings)

    # Align the subplots a little nicer, make more use of space. `hspace`: The
    # amount of height reserved for space between subplots, expressed as a
    # fraction of the average axis height
    plt.subplots_adjust(
        hspace=0.05, left=0.05, right=0.97, bottom=0.1, top=0.95)
    #plt.tight_layout()
    savefig(column_dict['plot_title'])


def plot_subplot(ax, column_dict, series, plotsettings):

    log.info('Plot column %s from %s', column_dict, series)

    # Set currently active axis to axis object handed over to this function.
    # That makes df.plot() add the data to said axis.
    plt.sca(ax)

    # Plot raw samples (1s CPU load averages as determined by pidstat).
    ax = series.plot(
        #linestyle='dashdot',
        linestyle='None',
        color='gray',
        marker='.',
        markersize=3,
        markeredgecolor='gray'
    )

    # Conditionally create a rolling window average plot on top of the raw
    # samples.
    window_width_seconds = int(column_dict['rolling_wdw_width_seconds'])
    if window_width_seconds != 0:

        # The raw samples are insightful, especially for seeing the outliers in
        # the distribution. However, also plot a rolling window average. It
        # shows where the distribution has its weight.
        rolling_average_series = series.rolling(
            '%ss' % window_width_seconds).sum() / window_width_seconds

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
        rolling_average_series.index = rolling_average_series.index - offset

        rolling_average_series.plot(
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
        ax.set_ylabel(column_dict['y_label'], fontsize=12)

    # The legend story is shitty with pandas intertwined w/ mpl.
    # http://stackoverflow.com/a/30666612/145400
    if plotsettings['show_legend']:
        legend = ['raw samples']
        if window_width_seconds != 0:
            legend.append('%s s rolling window average' % window_width_seconds)
        ax.legend(legend, numpoints=4)

    if 'ylim' in plotsettings:
        ax.set_ylim(plotsettings['ylim'])

    # Add tiny series_label label in the top-left corner of the subplot.
    ax.text(
        0.01, 0.88,
        plotsettings['series_label'],
        verticalalignment='center',
        fontsize=8,
        transform=ax.transAxes
    )


def parse_datafile_into_dataframe(datafilepath):

    log.info('Read input from from: %s', datafilepath)

    # column_names = [
    #     'time', 'uid', 'pid', 'cpu_usr_pcnt', 'cpu_system_pcnt',
    #     'cpu_guest_pcnt', 'cpu_pcnt', 'cpu_id', 'disk_kb_rd_s',
    #     'disk_kb_wr_s', 'disk_kb_ccwr_s', 'command'
    # ]

    # df = pd.read_csv(
    #     datafilepath,
    #     delim_whitespace=True,
    #     comment='#',
    #     header=None,
    #     names=column_names,
    #     index_col=0
    # )

    df = pd.read_csv(
        datafilepath,
        comment='#',
        index_col=0
    )

    # Now the dataframe contains an integer index, with unix timestamps. Parse
    # those into a DateTimeIndex, and replace the dataframe's index with the new
    # one.
    timestamps = pd.to_datetime(df.index)
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

    starttime = timestamps[0]
    log.info('Starttime with tz: %s', starttime)
    if starttime.tzinfo:
        local_starttime = (starttime - starttime.utcoffset()).replace(tzinfo=None)
        log.info('Starttime (local time): %s', local_starttime)

    timespan = timestamps[-1] - starttime
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
