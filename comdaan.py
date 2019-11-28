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
from statsmodels.nonparametric.smoothers_lowess import lowess
from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrule, MONTHLY, WEEKLY
from multiprocessing.pool import Pool

from gitparsing import _GitParser
from mailparsing import _MailParser
from issuesparsing import _IssuesParser

Activity = namedtuple("Activity", ["dataframe", "authors"])
TeamSize = namedtuple("TeamSize", ["dataframe"])
Network = namedtuple("Network", ["dataframe"])
Centrality = namedtuple("Centrality", ["centrality", "activity", "size"])
Response = namedtuple("Response", ["unanswered_issues", "response_time"])


def _network_from_dataframe(dataframe, author_col_name, target_col_name, source_col_name):
    if dataframe.empty:
        return nx.empty_graph()
    # In the case of issues and "discussion" needs processing
    if isinstance(dataframe[target_col_name].iloc[0], list) and isinstance(dataframe[target_col_name].iloc[0][0], dict):
        dataframe[target_col_name] = dataframe[target_col_name].apply(
            lambda discussion: [comment[author_col_name] for comment in discussion]
        )

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
        if not edge_list.empty:
            edge_list["weight"] = edge_list.apply(
                lambda x: len(
                    targets.loc[x["source"]][target_col_name].intersection(targets.loc[x["target"]][source_col_name])
                ),
                axis=1,
            )
        else:
            edge_list = edge_list.reindex(edge_list.columns.tolist() + ["weight"], axis=1)

    graph = nx.convert_matrix.from_pandas_edgelist(edge_list, edge_attr=["weight"])
    no_edges = []
    for u, v, weight in graph.edges.data("weight"):
        if weight == 0:
            no_edges.append((u, v))

    graph.remove_edges_from(no_edges)

    return graph


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


def teamsize(dataframe, id_col_name, author_col_name, date_col_name, actor="reporter", frac=None):
    author_team_size = pd.DataFrame()
    comm_team_size = pd.DataFrame()

    if "reporter" in actor:
        dataframe[date_col_name] = dataframe[date_col_name].apply(lambda x: x.date())
        dataframe[date_col_name] = pd.DatetimeIndex(dataframe[date_col_name]).to_period("W").to_timestamp()
        dataframe[date_col_name] = dataframe[date_col_name].apply(lambda x: x - timedelta(days=3))

        dataframe_by_date = dataframe.groupby(date_col_name)

        author_team_size["entry_count"] = dataframe_by_date[id_col_name].count()
        author_team_size["author_count"] = dataframe_by_date[author_col_name].nunique()

    if "commenter" in actor:
        dataframe = dataframe.explode("discussion").rename(columns={"discussion": "comment"})
        dataframe["comment_author"] = dataframe["comment"].apply(lambda comment: comment[author_col_name])
        dataframe["comment_created_at"] = dataframe["comment"].apply(lambda comment: comment[date_col_name])

        comments = dataframe.loc[:, ["comment_author", "comment_created_at"]].rename(
            columns={"comment_author": author_col_name, "comment_created_at": date_col_name}
        )
        comments[date_col_name] = comments[date_col_name].apply(lambda x: x.date())
        comments[date_col_name] = pd.DatetimeIndex(comments[date_col_name]).to_period("W").to_timestamp()
        comments[date_col_name] = comments[date_col_name].apply(lambda x: x - timedelta(days=3))
        comments[id_col_name] = comments.index

        comments_by_date = comments.groupby(date_col_name)

        comm_team_size["entry_count"] = comments_by_date[id_col_name].count()
        comm_team_size["author_count"] = comments_by_date[author_col_name].nunique()

    team_size = pd.concat([author_team_size, comm_team_size], sort=False)
    team_size = team_size.groupby(date_col_name).sum()
    team_size = team_size.sort_values(by=date_col_name)
    team_size.reset_index(inplace=True)

    y_a = team_size["entry_count"].values
    y_ac = team_size["author_count"].values
    x = team_size[date_col_name].apply(lambda date: date.timestamp()).values

    frac = float(frac) if frac is not None else 10 * len(x) ** (-0.75)

    team_size["entry_count_lowess"] = lowess(y_a, x, is_sorted=True, frac=frac if frac < 1 else 0.8, it=0)[:, 1]
    team_size["author_count_lowess"] = lowess(y_ac, x, is_sorted=True, frac=frac if frac < 1 else 0.8, it=0)[:, 1]

    return TeamSize(team_size)


# If the source and target columns are the same, only the source needs to be given.
def network(dataframe, author_col_name, target_col_name, source_col_name=None):
    graph = _network_from_dataframe(dataframe, author_col_name, target_col_name, source_col_name)
    degrees = nx.degree_centrality(graph)
    nodes = pd.DataFrame.from_records([degrees]).transpose()
    nodes.columns = ["centrality"]

    return Network(nodes)


def centrality(
    dataframe, id_col_name, author_col_name, date_col_name, target_col_name, source_col_name=None, name=None, frac=None
):
    authors = list(dataframe[author_col_name].sort_values().unique())
    if not name or authors.count(name) == 0:
        return authors
    dataframe[date_col_name] = dataframe[date_col_name].apply(lambda x: datetime(year=x.year, month=x.month, day=1))
    window_radius = 1
    delta = relativedelta(months=window_radius)
    freq = MONTHLY
    min_date = dataframe[date_col_name].min()
    max_date = dataframe[date_col_name].max()
    # Reducing the date interval by two months is problematic when the data source spans over less than two months.
    if max_date - relativedelta(months=2 * window_radius) < min_date:
        delta = relativedelta(weeks=window_radius)
        freq = WEEKLY

    min_date = min_date + delta
    max_date = max_date - delta

    date_range = rrule(freq=freq, dtstart=min_date, until=max_date)
    dates = [(date - delta, date + delta) for date in date_range]

    # Compensating the difference between rrule's last date and the actual max date
    last_date_in_df = dataframe[date_col_name].max()
    last_date_in_list = dates[-1][-1]
    if last_date_in_list < last_date_in_df:
        dates.append((last_date_in_list, last_date_in_df))

    degrees = []
    sizes = []
    with Pool() as pool:
        results = []
        for start_date, end_date in dates:
            mask = (dataframe[date_col_name] >= start_date) & (dataframe[date_col_name] <= end_date)
            results.append(
                pool.apply_async(
                    _network_from_dataframe,
                    args=(dataframe.loc[mask], author_col_name, target_col_name, source_col_name),
                )
            )
        for result in results:
            graph = result.get()
            degrees.append(nx.degree_centrality(graph))
            sizes.append(graph.number_of_nodes())

    date_x = [date for (date, x) in dates]
    x = list(map(lambda date: date.timestamp(), date_x))
    nodes = pd.DataFrame.from_records(degrees, index=date_x)
    nodes.index.name = date_col_name
    nodes.fillna(0.0, inplace=True)
    frac = float(frac) if frac is not None else 10 * len(x) ** (-0.75)
    nodes[name] = lowess(nodes[name], x, is_sorted=True, frac=frac if frac < 1 else 0.8, it=0)[:, 1]

    size_df = pd.DataFrame(data={"value": sizes}, index=date_x)
    size_df.index.name = date_col_name
    size_df = size_df / size_df.max()
    size_df.reset_index(inplace=True)
    x = size_df[date_col_name].apply(lambda date: date.timestamp())
    size_df["value"] = lowess(size_df["value"], x, is_sorted=True, frac=frac if frac < 1 else 0.8, it=0)[:, 1]

    activity = (
        dataframe.loc[:, [author_col_name, date_col_name, id_col_name]]
        .groupby([author_col_name, date_col_name])
        .count()
    )
    activity.columns = ["count"]
    activity = activity.unstack(level=0)
    activity.columns = [name for (x, name) in activity.columns]
    activity.fillna(0.0, inplace=True)
    activity = activity / activity.max()

    activity_df = pd.DataFrame(activity[name])
    activity_df.columns = ["value"]
    activity_df.reset_index(inplace=True)
    x = activity_df[date_col_name].apply(lambda date: date.timestamp())
    activity_df["value"] = lowess(activity_df["value"], x, is_sorted=True, frac=frac if frac < 1 else 0.8, it=0)[:, 1]

    centrality_df = pd.DataFrame(nodes[name])
    centrality_df.columns = ["value"]
    centrality_df.reset_index(inplace=True)

    return Centrality(centrality_df, activity_df, size_df)


def response(issues, id_col_name, author_col_name, date_col_name, discussion_col_name, frac=None):
    issues = issues.sort_values(by=date_col_name)
    issues = issues.reset_index(drop=True)

    def filter_notes(issue):
        for comment in issue[discussion_col_name]:
            if comment["system"] and comment[author_col_name] != issue[author_col_name]:
                return comment[date_col_name]
        return None  # Issues that are not answered yet

    def get_rates(issue, issues):
        answered = 0
        for index, i in issues.iterrows():
            if not pd.isna(i[discussion_col_name]) and i[discussion_col_name] <= issue[date_col_name]:
                answered += 1
            # id is a unique identifier and so ensures issue and i are the same
            if issue[id_col_name] == i[id_col_name]:
                return index - answered + 1  # Indices start at 0
        return None

    issues[discussion_col_name] = issues.apply(filter_notes, axis=1)
    issues["unanswered_to_this_date"] = issues.apply(get_rates, args=(issues,), axis=1)
    issues_answered = issues[pd.notnull(issues[discussion_col_name])]

    response_time = pd.DataFrame()
    response_time[date_col_name] = issues_answered[date_col_name]
    response_time["response_time"] = (
        issues_answered[discussion_col_name] - issues_answered[date_col_name]
    ) / timedelta(hours=1)

    y_rt = response_time["response_time"].values
    x = response_time[date_col_name].apply(lambda date: date.timestamp()).values

    frac = float(frac) if frac is not None else 10 * len(x) ** (-0.75)
    response_time["response_time_lowess"] = lowess(y_rt, x, is_sorted=True, frac=frac if frac < 1 else 0.8, it=0)[:, 1]

    response_time["response_time_formatted"] = response_time["response_time"].apply(
        lambda x: "{} day(s) and {} hour(s)".format(int(x // 24), int(x % 24))
    )

    return Response(issues.loc[:, [date_col_name, "unanswered_to_this_date"]], response_time)
