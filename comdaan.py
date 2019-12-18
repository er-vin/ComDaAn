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
from datetime import datetime, timedelta
from itertools import combinations
from functools import reduce

from bokeh.layouts import gridplot
from statsmodels.nonparametric.smoothers_lowess import lowess
from dateutil.relativedelta import relativedelta
from dateutil.rrule import rrule, MONTHLY, WEEKLY
from multiprocessing.pool import Pool
from bokeh.io import output_file
from bokeh.plotting import show

from gitparsing import _GitParser
from mailparsing import _MailParser
from issuesparsing import _IssuesParser
from object_types import Activity, Network, Centrality, TeamSize, Response
from display import _display


def _network_from_dataframe(dataframe, author_col_name, target_col_name, source_col_name):
    if dataframe.empty:
        return nx.empty_graph()
    # In the case of issues and "discussion" needs processing
    if "iid" in dataframe:
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
                    targets.loc[x["source"]][source_col_name].intersection(targets.loc[x["target"]][target_col_name])
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
    """
    This function parses a git repository or git repositories.

    :param paths: path or list of paths of git repositories to parse.
    :type paths: str or list or str
    :param start_date: Considering entries created after start_date. It should follow the "YYYY-MM-DD" format.
    :type start_date: str
    :param end_date: Considering entries created before end_date. It should follow the "YYYY-MM-DD" format.
    :type start_date: str
    :return: pandas.DataFrame containing all entries of the repositories.
    """

    parser = _GitParser()
    parser.add_repositories(paths)
    return parser.get_log(start_date, end_date)


def parse_mail(paths, start_date=None, end_date=None):
    """
    This function parses mailing lists in MBOX format.

    :param paths: path or list of paths of MBOX files to parse.
    :type paths: str or list or str
    :param start_date: Considering messages sent after start_date. It should follow the "YYYY-MM-DD" format.
    :type start_date: str
    :param end_date: Considering messages sent before end_date. It should follow the "YYYY-MM-DD" format.
    :type start_date: str
    :return: pandas.DataFrame containing all messages of the mailing lists.
    """

    parser = _MailParser()
    parser.add_archives(paths)
    return parser.get_emails(start_date, end_date)


def parse_issues(paths, start_date=None, end_date=None):
    """
    This function parses GitLab Issues stored in JSON files.

    :param paths: path or list of paths of JSON files to parse.
    :type paths: str or list or str
    :param start_date: Considering issues created after start_date. It should follow the "YYYY-MM-DD" format.
    :type start_date: str
    :param end_date: Considering issues created before end_date. It should follow the "YYYY-MM-DD" format.
    :type start_date: str
    :return: pandas.DataFrame containing all issues.
    """

    parser = _IssuesParser()
    parser.add_issues_paths(paths)
    return parser.get_issues(start_date, end_date)


def parse_comments(issues):
    """
    This function parses the comments of an issues DataFrame. It creates a DataFrame that stores all the comments in
    *issues*.

    :param issues: DataFrame containing the issues from which the comments are to be extracted.
    :type issues: pandas.DataFrame
    :return: pandas.DataFrame with the comment fields as columns.
    """

    exploded_df = issues.explode("discussion").rename(columns={"discussion": "comment"})
    commenter_df = pd.DataFrame(exploded_df["comment"].to_list())
    commenter_df.reset_index(inplace=True)
    return commenter_df


# In the case of commenter activity, id_col_name, author_col_name and date_col_name, are the names of the corresponding
# fields in dateframe["discussion"]. With parse_issues, they are the same as the ones directly in the dataframe.
def activity(dataframe, id_col_name, author_col_name, date_col_name):
    """
    This function runs an activity analysis on the dataset provided. It explores the weekly activity of the members of a
    team or community.

    In the case of issues, this analysis only considers bug reporters. To consider the commenters, the issues
    dataframe's comments can be parsed using the parse_comments function. Said function generates a dataframe with the
    needed columns and so can be used here. To consider both commenters and reporters, the issues and comments
    dataframes can be merged, both having the necessary columns.

    :param dataframe: DataFrame containing the data on which to conduct the activity analysis.
        It must contain at least an *id*, a *name* and a *date* column.
    :type dataframe: pandas.DataFrame
    :param id_col_name: Name of the column containing unique identifiers for each entry.
    :type id_col_name: str
    :param author_col_name: Name of the column containing the authors of the entries.
    :type author_col_name: str
    :param date_col_name: Name of the column containing the dates of the entries.
    :type date_col_name: str
    :return: Object of type Activity containing a *dataframe* field and an *authors* one.
    """

    dataframe[date_col_name] = dataframe[date_col_name].apply(lambda x: datetime(year=x.year, month=x.month, day=x.day))

    start_dates = dataframe.groupby(author_col_name)[author_col_name, date_col_name].min()
    start_dates.index.name = "author_name_index"
    authors = (
        start_dates.sort_values([date_col_name, author_col_name], ascending=False).loc[:, author_col_name].tolist()
    )
    authors = list(set(authors))

    daily_activity = (
        dataframe.loc[:, [author_col_name, date_col_name, id_col_name]]
        .groupby([author_col_name, date_col_name])
        .count()
    )
    daily_activity.columns = ["count"]

    weekly_activity = daily_activity.groupby(author_col_name).resample("W", level=1).sum()
    weekly_activity = weekly_activity.loc[lambda x: x["count"] > 0]
    weekly_activity = weekly_activity.reset_index(level=[author_col_name, date_col_name])
    weekly_activity[date_col_name] = weekly_activity[date_col_name].apply(lambda x: x - timedelta(days=3))
    weekly_activity["week_name"] = weekly_activity[date_col_name].apply(lambda x: "%s-%s" % x.isocalendar()[:2])

    weekly_activity = weekly_activity.rename(columns={author_col_name: "name", date_col_name: "date"})
    return Activity(weekly_activity, authors)


def teamsize(dataframe, id_col_name, author_col_name, date_col_name, frac=None):
    """
    This function runs a teamsize analysis on the dataset provided. It explores the evolution of the size and activity
    of a community or a team over time.

    In the case of issues, this analysis only considers bug reporters. To consider the commenters, the issues
    dataframe's comments can be parsed using the parse_comments function. Said function generates a dataframe with the
    needed columns and so can be used here. To consider both commenters and reporters, the issues and comments
    dataframes can be merged, both having the necessary columns.

    :param dataframe: DataFrame containing the data on which to conduct the activity analysis.
        It must contain at least an *id*, a *name* and a *date* column.
    :type dataframe: pandas.DataFrame
    :param id_col_name: Name of the column containing unique identifiers for each entry.
    :type id_col_name: str
    :param author_col_name: Name of the column containing the authors of the entries.
    :type author_col_name: str
    :param date_col_name: Name of the column containing the dates of the entries.
    :type date_col_name: str
    :param frac: The fraction of data to use for the curve smoothing factor.
    :type frac: float
    :return: Object of type TeamSize containing a *dataframe* field.
    """

    dataframe[date_col_name] = dataframe[date_col_name].apply(lambda x: x.date())
    dataframe[date_col_name] = pd.DatetimeIndex(dataframe[date_col_name]).to_period("W").to_timestamp()
    dataframe[date_col_name] = dataframe[date_col_name].apply(lambda x: x - timedelta(days=3))

    dataframe_by_date = dataframe.groupby(date_col_name)

    team_size = pd.DataFrame()

    team_size["entry_count"] = dataframe_by_date[id_col_name].count()
    team_size["author_count"] = dataframe_by_date[author_col_name].nunique()

    team_size = team_size.groupby(date_col_name).sum()
    team_size = team_size.sort_values(by=date_col_name)
    team_size.reset_index(inplace=True)

    y_a = team_size["entry_count"].values
    y_ac = team_size["author_count"].values
    x = team_size[date_col_name].apply(lambda date: date.timestamp()).values

    frac = float(frac) if frac is not None else 10 * len(x) ** (-0.75)

    team_size["entry_count_lowess"] = lowess(y_a, x, is_sorted=True, frac=frac if frac < 1 else 0.8, it=0)[:, 1]
    team_size["author_count_lowess"] = lowess(y_ac, x, is_sorted=True, frac=frac if frac < 1 else 0.8, it=0)[:, 1]
    team_size = team_size.rename(columns={date_col_name: "date"})
    return TeamSize(team_size)


# If the source and target columns are the same, only the source needs to be given.
def network(dataframe, author_col_name, target_col_name, source_col_name=None):
    """
    This function runs a Network analysis on the dataset provided.

    :param dataframe: DataFrame containing the data on which to conduct the activity analysis.
        It must contain at least an *author*, a *target* and a *source* column.
    :type dataframe: pandas.DataFrame
    :param author_col_name: Name of the column containing the authors of the entries.
    :type author_col_name: str
    :param target_col_name: Name of the column containing the targets of the relationship that the network analysis is
        supposed to exploring.
    :type target_col_name: str
    :param source_col_name: Name of the column containing the sources of the relationships that the network analysis is
        supposed to be exploring.
    :type source_col_name: str
    :return: Object of type network containing a *dataframe* field and a *graph* one.
    """

    graph = _network_from_dataframe(dataframe, author_col_name, target_col_name, source_col_name)
    degrees = nx.degree_centrality(graph)
    nodes = pd.DataFrame.from_records([degrees]).transpose()
    nodes.columns = ["centrality"]

    return Network(nodes, graph)


def centrality(
    dataframe, id_col_name, author_col_name, date_col_name, target_col_name, source_col_name=None, name=None, frac=None
):
    """
    This function runs a Centrality analysis on the dataset provided. It explores the evolution of an individuals
    centrality over time as well as their activity and the size of their team or community.

    :param dataframe: DataFrame containing the data on which to conduct the activity analysis.
        It must contain at least an *id*, a *name*, a *date*, a *target* and a *source* column.
    :type dataframe: pandas.DataFrame
    :param id_col_name: Name of the column containing unique identifiers for each entry.
    :type id_col_name: str
    :param author_col_name: Name of the column containing the authors of the entries.
    :type author_col_name: str
    :param date_col_name: Name of the column containing the dates of the entries.
    :type date_col_name: str
    :param target_col_name: Name of the column containing the targets of the relationship that the network analysis is
        supposed to exploring.
    :type target_col_name: str
    :param source_col_name: Name of the column containing the sources of the relationships that the network analysis is
        supposed to be exploring.
    :type source_col_name: str
    :param name: Name of the author whose centrality is to analyze.
    :type name: str
    :param frac: The fraction of data to use for the curve smoothing factor.
    :type frac: float
    :return: Object of type Centrality containing a *dataframe* field.
    """

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

    return Centrality(
        centrality_df.rename(columns={date_col_name: "date"}),
        activity_df.rename(columns={date_col_name: "date"}),
        size_df.rename(columns={date_col_name: "date"}),
        name,
    )


def response(issues, id_col_name, author_col_name, date_col_name, discussion_col_name, frac=None):
    """
    This function runs an issue response time analysis on the dataset provided. It returns the number of unanswered
    issues at each point in time as well as a curve representing the evolution of the reponse time to the issues of a
    certain project or community.

    :param issues: DataFrame containing the issues on which to conduct the response analysis.
        It must contain at least an *id*, a *name*, a *date* and a *discussion* column.
    :type issues: pandas.DataFrame
    :param id_col_name: Name of the column containing unique identifiers for each entry.
    :type id_col_name: str
    :param author_col_name: Name of the column containing the authors of the entries.
    :type author_col_name: str
    :param date_col_name: Name of the column containing the dates of the entries.
    :type date_col_name: str
    :param discussion_col_name: Name of the discussion column in the issues DataFrame.
    :type discussion_col_name: str
    :param frac: The fraction of data to use for the curve smoothing factor.
    :type frac: float
    :return: Object of type Response containing an *unanswered_issues* field and *response_time* one.
    """

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
    issues = issues.rename(columns={date_col_name: "date"})
    response_time = response_time.rename(columns={date_col_name: "date"})
    return Response(issues.loc[:, ["date", "unanswered_to_this_date"]], response_time)


def display(objects, title=None, output="result.html", palette="magma256"):
    """
    This function displays the results of the analyses.

    :param objects: An object of type Activity, TeamSize, Network, Centrality or Response or a list of such objects.
    :type objects: Activity, TeamSize, Network, Centrality or Response or a list of them.
    :param title: Title of the figure to display.
    :type title: str
    :param output: Output HTML file, default is *result.html*.
    :type output: str
    :param palette: Name of the bokeh palette to use.
    :type palette: str
    :return: No return value but opens the HTML file with the results.
    """

    if not isinstance(objects, list):
        objects = [objects]
    if palette != "magma256" and palette != "blue4":
        raise NameError("{} palette not found. Please choose either 'magma256' or 'blue4'".format(palette))
    output_file(output)

    # Grouping objects by their types
    accumulation = {}
    for objs in objects:
        accumulation.setdefault(type(objs), []).append(objs)
    objects_by_type = accumulation.values()

    plots = []
    for objs in objects_by_type:
        p = _display(objs, title, palette)
        plots.append(p)

    gp = gridplot(plots, ncols=2, sizing_mode="stretch_both")
    show(gp)
