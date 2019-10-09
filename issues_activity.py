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
from itertools import chain
from argparse import ArgumentParser

from pytz import utc

from issuesparsing import IssuesParser
from datetime import datetime, timedelta
from bokeh.plotting import figure, show
from bokeh.models import HoverTool, LinearColorMapper
from bokeh.models.sources import ColumnDataSource
from bokeh.palettes import Magma256
from bokeh.io import output_file

if __name__ == "__main__":
    # Parse the args before all else
    arg_parser = ArgumentParser(
        description="A tool for visualizing week by week, who's been most active regarding project issues.",
        parents=[IssuesParser.get_argument_parser()],
    )
    arg_parser.add_argument("-r", "--reporters", help="Display reporter activity.", action="store_true")

    arg_parser.add_argument("-c", "--commenters", help="Display commenter activity.", action="store_true")

    arg_parser.add_argument(
        "--palette", choices=["blue4", "magma256"], default="magma", help="Choose a palette (default is magma256)"
    )
    arg_parser.add_argument("-t", "--title", help="Title")
    arg_parser.add_argument("-o", "--output", help="Output file (default is 'result.html')")
    args = arg_parser.parse_args()

    start_date = args.start
    end_date = args.end

    comm = args.commenters
    rep = args.reporters

    if not rep and not comm:
        print("Please choose activity kind(s) to display.")
        exit(1)

    output_filename = args.output or "result.html"
    parser = IssuesParser()
    parser.add_issues_paths(args.paths)

    issues = parser.get_issues(start_date, end_date)
    issues["created_at"] = issues["created_at"].apply(lambda x: datetime(year=x.year, month=x.month, day=x.day))

    authors = []
    activity_data = pd.DataFrame()

    if rep:
        start_dates = issues.groupby("author")["author", "created_at"].min()
        start_dates.index.name = "author_index"
        authors += start_dates.sort_values(["created_at", "author"], ascending=False).loc[:, "author"].tolist()
        activity_data = issues.loc[:, ["author", "created_at", "id"]]

    if comm:

        # Merging all comments of multiple threads in the same big list.
        def get_thread_comments(discussion):
            comments = []
            for thread in discussion:
                comments += thread
            return comments

        issues["discussion"] = issues["discussion"].apply(get_thread_comments)
        issues = issues.explode("discussion").rename(columns={"discussion": "comment"})
        issues["comment_author"] = issues["comment"].apply(lambda comment: comment["author"]["name"])
        issues["comment_created_at"] = issues["comment"].apply(
            lambda comment: datetime.strptime(comment["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ").astimezone(utc)
        )
        commenters = issues.loc[:, ["comment_author", "comment_created_at"]].rename(
            columns={"comment_author": "author", "comment_created_at": "created_at"}
        )
        commenters["created_at"] = commenters["created_at"].apply(
            lambda x: datetime(year=x.year, month=x.month, day=x.day)
        )
        commenters["id"] = commenters.index
        authors += commenters.sort_values(["created_at", "author"], ascending=False).loc[:, "author"].tolist()
        activity_data = pd.concat([activity_data, commenters])

    authors = list(set(authors))
    daily_activity = activity_data.groupby(["author", "created_at"]).count()
    daily_activity.columns = ["count"]

    weekly_activity = daily_activity.groupby("author").resample("W", level=1).sum()
    weekly_activity = weekly_activity.loc[lambda x: x["count"] > 0]
    weekly_activity = weekly_activity.reset_index(level=["author", "created_at"])
    weekly_activity["created_at"] = weekly_activity["created_at"].apply(lambda x: x - timedelta(days=3))
    weekly_activity["week_name"] = weekly_activity["created_at"].apply(lambda x: "%s-%s" % x.isocalendar()[:2])

    palette = list(reversed(Magma256))
    color_mapper = LinearColorMapper(
        palette=palette, low=weekly_activity["count"].min(), high=weekly_activity["count"].max()
    )
    if args.palette == "blue4":
        palette = ["#EAF5F9", "#D6EBF2", "#C1E2EC", "#ADD8E6"]
        color_mapper = LinearColorMapper(palette=palette, low=0, high=4)

    output_file(output_filename)
    p = figure(
        x_axis_type="datetime",
        y_range=authors,
        sizing_mode="stretch_both",
        active_scroll="wheel_zoom",
        title=args.title,
    )
    p.add_tools(HoverTool(tooltips=[("Author", "@author"), ("Week", "@week_name"), ("Count", "@count")]))
    p.rect(
        "created_at",
        "author",
        source=ColumnDataSource(weekly_activity),
        fill_color={"field": "count", "transform": color_mapper},
        line_color={"field": "count", "transform": color_mapper},
        width=1000 * 60 * 60 * 24 * 7,
        height=1,
    )
    show(p)
