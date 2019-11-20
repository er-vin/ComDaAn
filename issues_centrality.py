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

from multiprocessing.pool import Pool

import pandas as pd
import networkx as nx

from argparse import ArgumentParser
from issuesparsing import IssuesParser
from bokeh.plotting import figure, show
from bokeh.palettes import Category10
from bokeh.models import HoverTool
from bokeh.models.annotations import Legend
from bokeh.models.sources import ColumnDataSource
from bokeh.io import output_file
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrule, MONTHLY
from statsmodels.nonparametric.smoothers_lowess import lowess


def network_from_dataframe(dataframe):
    authors = list(dataframe["author"])  # or toValues
    commenter_threads = list(dataframe["discussion"])
    edges = []

    for i in range(len(authors)):
        edges.extend([(authors[i], commenter) for commenter in commenter_threads[i]])

    edge_list = pd.DataFrame(edges, columns=["source", "target"])
    edge_list = edge_list.groupby(["source", "target"]).size().reset_index(name="weight")

    graph = nx.convert_matrix.from_pandas_edgelist(edge_list, edge_attr=["weight"])
    no_edges = []
    for u, v, weight in graph.edges.data("weight"):
        if weight == 0:
            no_edges.append((u, v))

    graph.remove_edges_from(no_edges)

    return graph


if __name__ == "__main__":
    # Parse the args before all else
    arg_parser = ArgumentParser(
        description="A tool for visualizing a contributor's activity and centrality over time",
        parents=[IssuesParser.get_argument_parser()],
    )
    arg_parser.add_argument(
        "--palette", choices=["blue4", "magma256"], default="magma", help="Choose a palette (default is magma256)"
    )
    arg_parser.add_argument(
        "-n",
        "--name",
        help="Name of the contributor to explore," + " if no name is provided a list of names will be proposed",
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

    # Merging all comments of multiple threads in the same big list.
    issues["discussion"] = issues["discussion"].apply(
        lambda discussion: [comment for thread in discussion for comment in thread]
    )
    # Filtering all data except commenter names.
    issues["discussion"] = issues["discussion"].apply(
        lambda discussion: [comment["author"]["name"] for comment in discussion]
    )

    issues["created_at"] = issues["created_at"].apply(lambda x: datetime(year=x.year, month=x.month, day=1))

    authors = list(issues["author"].sort_values().unique())
    if not args.name or authors.count(args.name) == 0:
        print("Found names: %s" % authors)
        exit(1)

    name = args.name

    min_date = issues["created_at"].min()
    max_date = issues["created_at"].max()
    window_radius = 1
    date_range = rrule(
        MONTHLY,
        dtstart=min_date + relativedelta(months=window_radius),
        until=max_date - relativedelta(months=window_radius),
    )
    dates = [
        (date - relativedelta(months=window_radius), date + relativedelta(months=window_radius)) for date in date_range
    ]

    degrees = []
    sizes = []
    with Pool() as pool:
        results = []
        for start_date, end_date in dates:
            mask = (issues["created_at"] >= start_date) & (issues["created_at"] <= end_date)
            results.append(pool.apply_async(network_from_dataframe, args=(issues.loc[mask],)))
        for result in results:
            graph = result.get()
            degrees.append(nx.degree_centrality(graph))
            sizes.append(graph.number_of_nodes())

    date_x = [date for (date, x) in dates]
    x = list(map(lambda date: date.timestamp(), date_x))
    nodes = pd.DataFrame.from_records(degrees, index=date_x)
    nodes.index.name = "created_at"
    nodes.fillna(0.0, inplace=True)
    frac = float(args.frac) if args.frac is not None else 10 * len(x) ** (-0.75)
    nodes[name] = lowess(nodes[name], x, is_sorted=True, frac=frac if frac < 1 else 0.8, it=0)[:, 1]

    size_df = pd.DataFrame(data={"value": sizes}, index=date_x)
    size_df.index.name = "created_at"
    size_df = size_df / size_df.max()
    size_df.reset_index(inplace=True)
    x = size_df["created_at"].apply(lambda date: date.timestamp())
    size_df["value"] = lowess(size_df["value"], x, is_sorted=True, frac=frac if frac < 1 else 0.8, it=0)[:, 1]

    activity = issues.loc[:, ["author", "created_at", "id"]].groupby(["author", "created_at"]).count()
    activity.columns = ["count"]
    activity = activity.unstack(level=0)
    activity.columns = [name for (x, name) in activity.columns]
    activity.fillna(0.0, inplace=True)
    activity = activity / activity.max()

    activity_df = pd.DataFrame(activity[name])
    activity_df.columns = ["value"]
    activity_df.reset_index(inplace=True)
    activity_df["created_at"] = activity_df["created_at"].apply(
        lambda date: date if date_x[0] <= date <= date_x[-1] else None
    )
    activity_df.dropna(inplace=True)
    x = activity_df["created_at"].apply(lambda date: date.timestamp())
    activity_df["value"] = lowess(activity_df["value"], x, is_sorted=True, frac=frac if frac < 1 else 0.8, it=0)[:, 1]

    centrality_df = pd.DataFrame(nodes[name])
    centrality_df.columns = ["value"]
    centrality_df.reset_index()

    output_file(output_filename)
    p = figure(x_axis_type="datetime", sizing_mode="stretch_both", active_scroll="wheel_zoom", title=name)
    p.xaxis.axis_label = "Date"

    p.add_layout(Legend(), "below")

    p.add_tools(
        HoverTool(
            tooltips=[("Date", "@date{%Y-%m}"), ("Value", "@value{(0.000)}")],
            formatters={"created_at": "datetime"},
            mode="vline",
        )
    )

    p.line(
        "created_at",
        "value",
        source=ColumnDataSource(centrality_df),
        line_width=2,
        color=Category10[3][0],
        legend="Centrality",
    )
    p.line(
        "created_at",
        "value",
        source=ColumnDataSource(activity_df),
        line_width=2,
        color=Category10[3][1],
        legend="Normalized Activity",
    )
    p.line(
        "created_at",
        "value",
        source=ColumnDataSource(size_df),
        line_width=2,
        color=Category10[3][2],
        legend="Normalized Size",
    )
    show(p)
