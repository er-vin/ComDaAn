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
from datetime import datetime, timedelta
from argparse import ArgumentParser
from pytz import utc

from issuesparsing import IssuesParser
from bokeh.plotting import figure, show
from bokeh.models import HoverTool, Range1d, LinearAxis
from bokeh.models.annotations import Legend
from bokeh.models.sources import ColumnDataSource
from bokeh.palettes import Category10
from bokeh.io import output_file


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
    args = arg_parser.parse_args()

    start_date = args.start
    end_date = args.end
    output_filename = args.output or "result.html"

    parser = IssuesParser()
    parser.add_issues_paths(args.paths)
    issues = parser.get_issues(start_date, end_date)
    issues = issues.sort_values(by="created_at")
    issues = issues.reset_index(drop=True)

    def filter_notes(issue):
        for thread in issue["discussion"]:
            for note in thread:
                if note["system"] and note["author"]["name"] != issue["author"]:
                    return datetime.strptime(note["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ").astimezone(utc)
        return None  # Issues that are not answered yet

    def get_rates(issue, issues):
        answered = 0
        for index, i in issues.iterrows():
            if not pd.isna(i["discussion"]) and i["discussion"] <= issue["created_at"]:
                answered += 1
            if issue["id"] == i["id"]:  # id is a unique identifier and so ensures issue and i are the same
                return index - answered + 1  # Indices start at 0
        return None

    issues["discussion"] = issues.apply(filter_notes, axis=1)
    issues["unanswered_to_this_date"] = issues.apply(get_rates, args=(issues,), axis=1)
    issues_answered = issues[pd.notnull(issues["discussion"])]

    response_time = pd.DataFrame()
    response_time["date"] = issues_answered["created_at"]
    response_time["response_time"] = (issues_answered["discussion"] - issues_answered["created_at"]) / timedelta(
        hours=1
    )
    smoothed = response_time.rolling(100, center=True, win_type="triang").mean()
    response_time["response_time_smooth"] = smoothed["response_time"]

    response_time["response_time_formatted"] = response_time["response_time"].apply(
        lambda x: "{} day(s) and {} hour(s)".format(int(x // 24), int(x % 24))
    )

    output_file(output_filename)
    p = figure(x_axis_type="datetime", sizing_mode="stretch_both", active_scroll="wheel_zoom", title=args.title)
    p.xaxis.axis_label = "Date"
    p.yaxis.axis_label = "Response Time (in hours)"

    p.extra_y_ranges = {"response_range": Range1d(start=0, end=issues.shape[0])}

    p.add_layout(LinearAxis(y_range_name="response_range", axis_label="Response Rate"), "right")

    p.add_layout(Legend(), "below")

    p.add_tools(
        HoverTool(
            tooltips=[("Date", "@date{%Y-w%V}"), ("Response Time", "@response_time_formatted")],
            formatters={"date": "datetime"},
            point_policy="snap_to_data",
        )
    )

    p.vbar(
        x=issues["created_at"],
        top=issues["unanswered_to_this_date"],
        width=0.4,
        color=Category10[3][1],
        y_range_name="response_range",
        legend="Unanswered",
    )

    p.circle(
        "date",
        "response_time",
        source=ColumnDataSource(response_time),
        color=Category10[3][0],
        fill_alpha=0.1,
        line_alpha=0.2,
    )

    p.line(
        "date",
        "response_time_smooth",
        source=ColumnDataSource(response_time),
        line_width=3,
        color=Category10[3][0],
        legend="Response time",
    )

    show(p)
