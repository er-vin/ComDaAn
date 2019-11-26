#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# Copyright 2018 Kevin Ottens <ervin@ipsquad.net>
#
# The authors license this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from argparse import ArgumentParser
from datetime import timedelta
from gitparsing import GitParser

from pandas import DataFrame, DatetimeIndex
from bokeh.plotting import figure, show
from bokeh.models import HoverTool, LinearAxis, Range1d
from bokeh.models.annotations import Legend
from bokeh.models.sources import ColumnDataSource
from bokeh.palettes import Category10
from bokeh.io import output_file

from statsmodels.nonparametric.smoothers_lowess import lowess


if __name__ == "__main__":
    # Parse the args before all else
    arg_parser = ArgumentParser(
        description="A tool for visualizing, month by month the team size and activity",
        parents=[GitParser.get_argument_parser()],
    )
    arg_parser.add_argument("-t", "--title", help="Title")
    arg_parser.add_argument("-o", "--output", help="Output file (default is 'result.html')")
    arg_parser.add_argument("-d", "--frac", help="The fraction of data used while estimating each y value")
    args = arg_parser.parse_args()

    start_date = args.start
    end_date = args.end
    output_filename = args.output or "result.html"

    parser = GitParser()
    parser.add_repositories(args.paths)
    log = parser.get_log(start_date, end_date)
    log["date"] = log["date"].apply(lambda x: x.date())
    log["date"] = DatetimeIndex(log["date"]).to_period("W").to_timestamp()
    log["date"] = log["date"].apply(lambda x: x - timedelta(days=3))

    log_by_date = log.groupby("date")
    team_size = DataFrame()
    team_size["date"] = log_by_date.indices
    team_size["commit_count"] = log_by_date["id"].count()
    team_size["authors_count"] = log_by_date["author_name"].nunique()

    y_cc = team_size["commit_count"].values
    y_ac = team_size["authors_count"].values
    x = team_size["date"].apply(lambda date: date.timestamp()).values

    # "frac" is the fraction of data used to estimate each value
    # Its values can be predicted accurately enough with the following power function. It was obtained by a regression
    # analysis of a number of data sets and their corresponding frac.
    frac = float(args.frac) if args.frac is not None else 10 * len(x) ** (-0.75)

    # "it" is the number of residual based reweightings. If it is equal to 2, then additional weighted local regressions
    # are performed where the weights are the same as the above weights times the lowess bisquare function of the
    # residuals.
    cc_lowess_regression = lowess(y_cc, x, is_sorted=True, frac=frac if frac < 1 else 0.8, it=0)
    ac_lowess_regression = lowess(y_ac, x, is_sorted=True, frac=frac if frac < 1 else 0.8, it=0)

    team_size["commit_count_lowess"] = cc_lowess_regression[:, 1]
    team_size["author_count_lowess"] = ac_lowess_regression[:, 1]

    output_file(output_filename)
    p = figure(x_axis_type="datetime", sizing_mode="stretch_both", active_scroll="wheel_zoom", title=args.title)
    p.xaxis.axis_label = "Date"
    p.yaxis.axis_label = "Commit Count"

    p.extra_y_ranges = {"team_range": Range1d(start=0, end=team_size["authors_count"].max())}
    p.add_layout(LinearAxis(y_range_name="team_range", axis_label="Team Size"), "right")

    p.add_layout(Legend(), "below")

    p.add_tools(
        HoverTool(
            tooltips=[("Date", "@date{%Y-w%V}"), ("Team Size", "@authors_count"), ("Commit Count", "@commit_count")],
            formatters={"date": "datetime"},
            point_policy="snap_to_data",
        )
    )

    p.circle(
        "date",
        "commit_count",
        source=ColumnDataSource(team_size),
        color=Category10[3][0],
        fill_alpha=0.1,
        line_alpha=0.2,
    )
    p.circle(
        "date",
        "authors_count",
        source=ColumnDataSource(team_size),
        y_range_name="team_range",
        color=Category10[3][1],
        fill_alpha=0.1,
        line_alpha=0.2,
    )

    p.line(
        "date",
        "commit_count_lowess",
        source=ColumnDataSource(team_size),
        line_width=2,
        color=Category10[3][0],
        legend="Commit Count",
    )
    p.line(
        "date",
        "author_count_lowess",
        source=ColumnDataSource(team_size),
        y_range_name="team_range",
        line_width=2,
        color=Category10[3][1],
        legend="Team Size",
    )

    show(p)
