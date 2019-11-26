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

import networkx as nx
import pandas as pd
from collections import namedtuple
from datetime import datetime, timedelta
from itertools import combinations
from functools import reduce

from gitparsing import _GitParser
from mailparsing import _MailParser
from issuesparsing import _IssuesParser

Activity = namedtuple("Activity", ["dataframe", "authors"])
Network = namedtuple("Network", ["dataframe"])


def parse_repositories(paths, start_date=None, end_date=None):
    parser = _GitParser()
    parser.add_repositories(paths)
    return parser.get_log(start_date, end_date)


def parse_mail(paths, start_date=None, end_date=None):
    parser = _MailParser()
    parser.add_archives(paths)
    return parser.get_emails(start_date, end_date)


def parse_issues(paths, start_date=None, end_date=None):
    parser = _IssuesParser()
    parser.add_issues_paths(paths)
    return parser.get_issues(start_date, end_date)


# In the case of commenter activity, id_col_name, author_col_name and date_col_name, are the names of the corresponding
# fields in dateframe["discussion"]. With parse_issues, they are the same as the ones directly in the dataframe.
def activity(dataframe, id_col_name, author_col_name, date_col_name, actor="reporter"):
    dataframe[date_col_name] = dataframe[date_col_name].apply(lambda x: datetime(year=x.year, month=x.month, day=x.day))

    authors = []
    activity_data = pd.DataFrame()

    if "reporter" in actor:
        start_dates = dataframe.groupby(author_col_name)[author_col_name, date_col_name].min()
        start_dates.index.name = "author_name_index"
        authors += (
            start_dates.sort_values([date_col_name, author_col_name], ascending=False).loc[:, author_col_name].tolist()
        )
        activity_data = dataframe.loc[:, [author_col_name, date_col_name, id_col_name]]

    if "commenter" in actor:
        dataframe = dataframe.explode("discussion").rename(columns={"discussion": "comment"})
        dataframe["comment_author"] = dataframe["comment"].apply(lambda x: x[author_col_name])
        dataframe["comment_created_at"] = dataframe["comment"].apply(lambda x: x[date_col_name])
        commenters = dataframe.loc[:, ["comment_author", "comment_created_at"]].rename(
            columns={"comment_author": author_col_name, "comment_created_at": date_col_name}
        )
        commenters[date_col_name] = commenters[date_col_name].apply(
            lambda x: datetime(year=x.year, month=x.month, day=x.day)
        )
        commenters[id_col_name] = commenters.index
        authors += (
            commenters.sort_values([date_col_name, author_col_name], ascending=False).loc[:, author_col_name].tolist()
        )
        activity_data = pd.concat([activity_data, commenters])

    authors = list(set(authors))
    daily_activity = activity_data.groupby([author_col_name, date_col_name]).count()
    daily_activity.columns = ["count"]

    weekly_activity = daily_activity.groupby(author_col_name).resample("W", level=1).sum()
    weekly_activity = weekly_activity.loc[lambda x: x["count"] > 0]
    weekly_activity = weekly_activity.reset_index(level=[author_col_name, date_col_name])
    weekly_activity[date_col_name] = weekly_activity[date_col_name].apply(lambda x: x - timedelta(days=3))
    weekly_activity["week_name"] = weekly_activity[date_col_name].apply(lambda x: "%s-%s" % x.isocalendar()[:2])

    return Activity(weekly_activity, authors)


# If the source and target columns are the same, only the source needs to be given.
def network(dataframe, author_col_name, target_col_name, source_col_name=None):
    # In the case of issues and "discussion" needs processing
    if isinstance(dataframe[target_col_name].iloc[0], list) and isinstance(dataframe[target_col_name].iloc[0][0], dict):
        dataframe[target_col_name] = dataframe[target_col_name].apply(
            lambda discussion: [comment[author_col_name] for comment in discussion])

        authors = list(dataframe[author_col_name])
        commenter_threads = list(dataframe[target_col_name])

        edges = []

        for i in range(len(authors)):
            edges.extend([(authors[i], commenter) for commenter in commenter_threads[i]])
        
        edge_list = pd.DataFrame(edges, columns=["source", "target"])
        edge_list = edge_list.groupby(["source", "target"]).size().reset_index(name="weight")

    else:  # In the cases of mail and git repositories
        def to_set(df, col):
            if not isinstance(df[col].iloc[0], set):
                if isinstance(df[col].iloc[0], list):
                    df[col] = df[col].apply(lambda x: set(x))
                else:
                    # If x isn't an iterable, applying set to it will break it down into one. For example, a str would
                    # a list of chars which is why we turn it into a list with only x in it and then into a set.
                    df[col] = df[col].apply(lambda x: set([x]))
            return df

        if source_col_name is None:
            dataframe = to_set(dataframe, target_col_name)
            groups = dataframe.loc[:, [author_col_name, target_col_name]].groupby(author_col_name)
            source_col_name = target_col_name
        else:
            dataframe = to_set(dataframe, target_col_name)
            dataframe = to_set(dataframe, source_col_name)
            groups = dataframe.loc[:, [author_col_name, target_col_name, source_col_name]].groupby(author_col_name)

        targets = groups.aggregate(lambda x: reduce(set.union, x))

        edges = list(combinations(targets.index.tolist(), 2))
        edge_list = pd.DataFrame(edges, columns=["source", "target"])
        edge_list["weight"] = edge_list.apply(
            lambda x: len(
                targets.loc[x["source"]][target_col_name].intersection(targets.loc[x["target"]][source_col_name])
            ),
            axis=1,
        )

    graph = nx.convert_matrix.from_pandas_edgelist(edge_list, edge_attr=["weight"])
    no_edges = []
    for u, v, weight in graph.edges.data("weight"):
        if weight == 0:
            no_edges.append((u, v))

    graph.remove_edges_from(no_edges)

    degrees = nx.degree_centrality(graph)
    nodes = pd.DataFrame.from_records([degrees]).transpose()
    nodes.columns = ["centrality"]

    return Network(nodes)
