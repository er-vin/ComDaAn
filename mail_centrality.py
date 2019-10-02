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
import networkx as nx

from argparse import ArgumentParser
from mailparsing import MailParser
from bokeh.plotting import figure, show
from bokeh.palettes import Category10
from bokeh.models import HoverTool
from bokeh.models.annotations import Legend
from bokeh.models.sources import ColumnDataSource
from bokeh.io import output_file
from itertools import combinations
from functools import reduce
from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrule, MONTHLY


def network_from_dataframe(dataframe):
    groups = dataframe.loc[:, ["sender_name", "message_id", "references"]].groupby("sender_name")
    # emails["references"] = emails["references"].apply(lambda x: set(x))
    # emails["message_id"] = emails["message_id"].apply(lambda x: set([x]))
    references = groups.aggregate(lambda x: reduce(set.union, x))

    edges = list(combinations(references.index.tolist(), 2))

    edge_list = pd.DataFrame(edges, columns=["source", "target"])

    # Measuring how much senders' messages reference other messages or are referenced to.
    # A more connected sender is one that references more messages and has their messages referenced to.
    edge_list["weight"] = edge_list.apply(
        lambda x: len(
            references.loc[x["source"]]["message_id"].intersection(references.loc[x["target"]]["references"])
        ),
        axis=1,
    )
    graph = nx.convert_matrix.from_pandas_edgelist(edge_list, edge_attr=["weight"], create_using=nx.DiGraph)
    no_edges = []
    for u, v, weight in graph.edges.data("weight"):
        if weight == 0:
            no_edges.append((u, v))

    graph.remove_edges_from(no_edges)

    return graph


if __name__ == "__main__":
    # Parse the args before all else
    arg_parser = ArgumentParser(
        description="A tool for exploring centrality and activity of a contributor to a mailing list over time",
        parents=[MailParser.get_argument_parser()],
    )
    arg_parser.add_argument(
        "-n",
        "--name",
        help="Name of the contributor to explore," + " if no name is provided a list of names will be proposed",
    )
    arg_parser.add_argument("-o", "--output", help="Output file (default is 'result.html')")
    args = arg_parser.parse_args()

    start_date = args.start
    end_date = args.end
    output_filename = args.output or "result.html"

    parser = MailParser()
    parser.add_archives(args.paths)
    emails = parser.get_emails(start_date, end_date)
    emails["references"] = emails["references"].apply(lambda x: set(x))
    emails["message_id"] = emails["message_id"].apply(lambda x: set([x]))

    senders = list(emails["sender_name"].sort_values().unique())
    if not args.name or senders.count(args.name) == 0:
        print("Found names: %s" % senders)
        exit(1)

    name = args.name
    window_radius = 1
    min_date = emails["date"].min()
    max_date = emails["date"].max()
    # Reducing the date interval by two months is problematic when the mailing list spans
    # over less than two months.
    if max_date - relativedelta(months=2) > min_date:
        min_date = emails["date"].min() + relativedelta(months=window_radius)
        max_date = emails["date"].max() - relativedelta(months=window_radius)
    date_range = rrule(freq=MONTHLY, dtstart=min_date, until=max_date)
    dates = [
        (date - relativedelta(months=window_radius), date + relativedelta(months=window_radius)) for date in date_range
    ]
    degrees = []
    sizes = []
    for start_date, end_date in dates:
        mask = (emails["date"] >= start_date) & (emails["date"] <= end_date)
        graph = network_from_dataframe(emails.loc[mask])
        degrees.append(nx.degree_centrality(graph))
        sizes.append(graph.number_of_nodes())

    # max date can't appear in results like that
    nodes = pd.DataFrame.from_records(degrees, index=[date for (date, x) in dates])
    nodes.index.name = "date"
    nodes.fillna(0.0, inplace=True)
    nodes = nodes.rolling(window=3).mean()

    size_df = pd.DataFrame(data={"value": sizes}, index=[date for (date, x) in dates])
    size_df.index.name = "date"
    size_df = size_df / size_df.max()
    size_df.reset_index()

    activity = emails.loc[:, ["sender_name", "date", "message_id"]].groupby(["sender_name", "date"]).count()
    activity.columns = ["count"]
    activity = activity.unstack(level=0)
    activity.columns = [name for (x, name) in activity.columns]
    activity = activity[2 * window_radius : -2 * window_radius]
    activity.fillna(0.0, inplace=True)
    activity = activity / activity.max()

    activity_df = pd.DataFrame(activity[name])
    activity_df.columns = ["value"]
    activity_df.reset_index()

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
            formatters={"date": "datetime"},
            mode="vline",
        )
    )

    p.line(
        "date",
        "value",
        source=ColumnDataSource(centrality_df),
        line_width=2,
        color=Category10[3][0],
        legend="Centrality",
    )
    p.line(
        "date",
        "value",
        source=ColumnDataSource(activity_df),
        line_width=2,
        color=Category10[3][1],
        legend="Normalized Activity",
    )
    p.line(
        "date",
        "value",
        source=ColumnDataSource(size_df),
        line_width=2,
        color=Category10[3][2],
        legend="Normalized Size",
    )
    show(p)
