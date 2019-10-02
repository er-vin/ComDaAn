#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# Copyright 2019 Christelle Zouein <christellezouein@hotmail.com>
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

from mailparsing import MailParser
from argparse import ArgumentParser
from datetime import timedelta

from pandas import DatetimeIndex, DataFrame
from bokeh.plotting import figure, show
from bokeh.models import HoverTool, LinearAxis, Range1d
from bokeh.models.annotations import Legend
from bokeh.models.sources import ColumnDataSource
from bokeh.palettes import Category10
from bokeh.io import output_file

if __name__ == "__main__":
    # Parse the args before all else
    arg_parser = ArgumentParser(
        description="A tool for visualizing, week by week the team size and activity",
        parents=[MailParser.get_argument_parser()],
    )
    arg_parser.add_argument("-t", "--title", help="Title")
    arg_parser.add_argument("-o", "--output", help="Output file (default is 'result.html')")
    args = arg_parser.parse_args()

    start_date = args.start
    end_date = args.end
    output_filename = args.output or "result.html"

    parser = MailParser()
    parser.add_archives(args.paths)
    emails = parser.get_emails(start_date, end_date)
    emails["date"] = emails["date"].apply(lambda x: x.date())
    emails["date"] = DatetimeIndex(emails["date"]).to_period("W").to_timestamp()
    emails["date"] = emails["date"].apply(lambda x: x - timedelta(days=3))

    emails_by_date = emails.groupby("date")

    team_size = DataFrame()
    team_size["message_count"] = emails_by_date["message_id"].count()
    team_size["sender_count"] = emails_by_date["sender_name"].nunique()  # number of unique senders

    smoothed = team_size.rolling(50, center=True, win_type="triang").mean()
    team_size["message_count_smooth"] = smoothed["message_count"]
    team_size["sender_count_smooth"] = smoothed["sender_count"]

    output_file(output_filename)

    p = figure(x_axis_type="datetime", sizing_mode="stretch_both", active_scroll="wheel_zoom", title=args.title)
    p.xaxis.axis_label = "Date"
    p.yaxis.axis_label = "Message Count"

    p.extra_y_ranges = {"team_range": Range1d(start=0, end=team_size["sender_count"].max())}
    p.add_layout(LinearAxis(y_range_name="team_range", axis_label="Team Size"), "right")

    p.add_layout(Legend(), "below")

    p.add_tools(
        HoverTool(
            tooltips=[("Date", "@date{%Y-w%V}"), ("Team Size", "@sender_count"), ("Message Count", "@message_count")],
            formatters={"date": "datetime"},
            point_policy="snap_to_data",
        )
    )

    p.circle(
        "date",
        "message_count",
        source=ColumnDataSource(team_size),
        color=Category10[3][0],
        fill_alpha=0.1,
        line_alpha=0.2,
    )
    p.circle(
        "date",
        "sender_count",
        source=ColumnDataSource(team_size),
        y_range_name="team_range",
        color=Category10[3][1],
        fill_alpha=0.1,
        line_alpha=0.2,
    )

    p.line(
        "date",
        "message_count_smooth",
        source=ColumnDataSource(team_size),
        line_width=2,
        color=Category10[3][0],
        legend="Message Count",
    )
    p.line(
        "date",
        "sender_count_smooth",
        source=ColumnDataSource(team_size),
        y_range_name="team_range",
        line_width=2,
        color=Category10[3][1],
        legend="Team Size",
    )
    show(p)
