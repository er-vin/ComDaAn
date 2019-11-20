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

import pandas as pd

from argparse import ArgumentParser
from datetime import timedelta, datetime
from pytz import utc

from issuesparsing import IssuesParser
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
        parents=[IssuesParser.get_argument_parser()],
    )
    arg_parser.add_argument(
        "--palette", choices=["blue4", "magma256"], default="magma", help="Choose a palette (default is magma256)"
    )
    arg_parser.add_argument("-t", "--title", help="Title")
    arg_parser.add_argument("-o", "--output", help="Output file (default is 'result.html')")
    arg_parser.add_argument("-d", "--frac", help="The fraction of data used while estimating each y value")
    args = arg_parser.parse_args()

    start_date = args.start
    end_date = args.end
    output_filename = args.output or "result.html"

    parser = IssuesParser()
    parser.add_issues_paths(args.paths)
    issues = parser.get_issues(start_date, end_date)

    issues["created_at"] = issues["created_at"].apply(lambda x: x.date())
    issues["created_at"] = pd.DatetimeIndex(issues["created_at"]).to_period("W").to_timestamp()
    issues["created_at"] = issues["created_at"].apply(lambda x: x - timedelta(days=3))

    issues_by_date = issues.groupby("created_at")

    author_team_size = pd.DataFrame()
    author_team_size["activity"] = issues_by_date["id"].count()
    author_team_size["authors_count"] = issues_by_date["author"].nunique()

    # Merging all comments of multiple threads in the same big list.
    def get_thread_comments(discussion):
        comments = []
        for thread in discussion:
            for comment in thread:
                comments.append(comment)
        return comments

    issues["discussion"] = issues["discussion"].apply(get_thread_comments)
    issues = issues.explode("discussion").rename(columns={"discussion": "comment"})
    issues["comment_author"] = issues["comment"].apply(lambda comment: comment["author"]["name"])
    issues["comment_created_at"] = issues["comment"].apply(
        lambda comment: datetime.strptime(comment["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ").astimezone(utc)
    )
    comments = issues.loc[:, ["comment_author", "comment_created_at"]].rename(
        columns={"comment_author": "author", "comment_created_at": "created_at"}
    )
    comments["created_at"] = comments["created_at"].apply(lambda x: x.date())
    comments["created_at"] = pd.DatetimeIndex(comments["created_at"]).to_period("W").to_timestamp()
    comments["created_at"] = comments["created_at"].apply(lambda x: x - timedelta(days=3))
    comments["id"] = comments.index

    comments_by_date = comments.groupby("created_at")

    comm_team_size = pd.DataFrame()
    comm_team_size["activity"] = comments_by_date["id"].count()
    comm_team_size["author_count"] = comments_by_date["author"].nunique()

    team_size = pd.concat([author_team_size, comm_team_size], sort=False)
    team_size = team_size.groupby("created_at").sum()
    team_size = team_size.sort_values(by="created_at")
    team_size.reset_index(inplace=True)

    y_a = team_size["activity"].values
    y_ac = team_size["author_count"].values
    x = team_size["created_at"].apply(lambda date: date.timestamp()).values

    frac = float(args.frac) if args.frac is not None else 10 * len(x) ** (-0.75)

    team_size["activity_lowess"] = lowess(y_a, x, is_sorted=True, frac=frac if frac < 1 else 0.8, it=0)[:, 1]
    team_size["author_count_lowess"] = lowess(y_ac, x, is_sorted=True, frac=frac if frac < 1 else 0.8, it=0)[:, 1]

    output_file(output_filename)
    p = figure(x_axis_type="datetime", sizing_mode="stretch_both", active_scroll="wheel_zoom", title=args.title)
    p.xaxis.axis_label = "Date"
    p.yaxis.axis_label = "Activity"

    p.extra_y_ranges = {"team_range": Range1d(start=0, end=team_size["authors_count"].max())}
    p.add_layout(LinearAxis(y_range_name="team_range", axis_label="Team Size"), "right")

    p.add_layout(Legend(), "below")

    p.add_tools(
        HoverTool(
            tooltips=[("Date", "@created_at{%Y-w%V}"), ("Team Size", "@authors_count"), ("Issue Count", "@activity")],
            formatters={"created_at": "datetime"},
            point_policy="snap_to_data",
        )
    )

    p.circle(
        "created_at",
        "activity",
        source=ColumnDataSource(team_size),
        color=Category10[3][0],
        fill_alpha=0.1,
        line_alpha=0.2,
    )
    p.circle(
        "created_at",
        "authors_count",
        source=ColumnDataSource(team_size),
        y_range_name="team_range",
        color=Category10[3][1],
        fill_alpha=0.1,
        line_alpha=0.2,
    )

    p.line(
        "created_at",
        "activity_lowess",
        source=ColumnDataSource(team_size),
        line_width=2,
        color=Category10[3][0],
        legend="Activity",
    )
    p.line(
        "created_at",
        "author_count_lowess",
        source=ColumnDataSource(team_size),
        y_range_name="team_range",
        line_width=2,
        color=Category10[3][1],
        legend="Team Size",
    )
    show(p)
