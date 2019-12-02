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

from bokeh.models import LinearColorMapper, MultiLine, Circle, TapTool, BoxSelectTool
from bokeh.models.sources import ColumnDataSource
from bokeh.palettes import Magma256, Spectral4, Magma11, Category10
from bokeh.models.graphs import from_networkx, NodesAndLinkedEdges
from bokeh.plotting import figure
from bokeh.layouts import gridplot
from bokeh.models import HoverTool
from bokeh.models import Range1d, LinearAxis, Legend

from object_types import Activity, Network, Centrality, Response, TeamSize


def _activity_figure(p, palette, weekly_activity):
    if palette == "blue4":
        palette = ["#EAF5F9", "#D6EBF2", "#C1E2EC", "#ADD8E6"]
        color_mapper = LinearColorMapper(palette=palette, low=0, high=4)
    else:
        palette = list(reversed(Magma256))
        color_mapper = LinearColorMapper(
            palette=palette, low=weekly_activity["count"].min(), high=weekly_activity["count"].max()
        )
    p.add_tools(HoverTool(tooltips=[("Author", "@name"), ("Week", "@week_name"), ("Count", "@count")]))
    p.rect(
        "date",
        "name",
        source=ColumnDataSource(weekly_activity),
        fill_color={"field": "count", "transform": color_mapper},
        line_color={"field": "count", "transform": color_mapper},
        width=1000 * 60 * 60 * 24 * 7,
        height=1,
    )
    return p


def _network_figure(p, nodes, graph):
    palette = list(reversed(Magma11))
    color_mapper = LinearColorMapper(palette=palette, low=nodes["centrality"].min(), high=nodes["centrality"].max())
    p.add_tools(HoverTool(tooltips=[("Name", "@index"), ("Centrality", "@centrality")]), TapTool(), BoxSelectTool())

    p.xaxis.visible = False
    p.yaxis.visible = False
    p.grid.visible = False

    renderer = from_networkx(graph, nx.kamada_kawai_layout)

    renderer.node_renderer.data_source.add(nodes["centrality"], "centrality")
    renderer.node_renderer.glyph = Circle(size=15, fill_color={"field": "centrality", "transform": color_mapper})
    renderer.node_renderer.selection_glyph = Circle(size=15, fill_color=Spectral4[2])
    renderer.node_renderer.hover_glyph = Circle(size=15, fill_color=Spectral4[1])

    renderer.edge_renderer.glyph = MultiLine(line_color="#CCCCCC", line_alpha=0.8, line_width=2)
    renderer.edge_renderer.selection_glyph = MultiLine(line_color=Spectral4[2], line_width=4)
    renderer.edge_renderer.hover_glyph = MultiLine(line_color=Spectral4[1], line_width=4)

    renderer.selection_policy = NodesAndLinkedEdges()
    renderer.inspection_policy = NodesAndLinkedEdges()

    p.renderers.append(renderer)
    return p


def _centrality_figure(p, obj, activity_color, centrality_color, size_color):
    centrality_df = obj.centrality
    activity_df = obj.activity
    size_df = obj.size
    p.line(
        "date",
        "value",
        source=ColumnDataSource(centrality_df),
        line_width=2,
        color=centrality_color,
        legend="{}: Centrality".format(obj.name),
    )
    p.line(
        "date",
        "value",
        source=ColumnDataSource(activity_df),
        line_width=2,
        color=activity_color,
        legend="{}: Normalized Activity".format(obj.name),
    )
    p.line(
        "date",
        "value",
        source=ColumnDataSource(size_df),
        line_width=2,
        color=size_color,
        legend="{}: Normalized Size".format(obj.name),
    )
    return p


def _teamsize_figure(p, team_size, entry_count_color, author_count_color):
    p.circle(
        "date",
        "entry_count",
        source=ColumnDataSource(team_size),
        color=entry_count_color,
        fill_alpha=0.1,
        line_alpha=0.2,
    )
    p.circle(
        "date",
        "author_count",
        source=ColumnDataSource(team_size),
        y_range_name="team_range",
        color=author_count_color,
        fill_alpha=0.1,
        line_alpha=0.2,
    )

    p.line(
        "date",
        "entry_count_lowess",
        source=ColumnDataSource(team_size),
        line_width=2,
        color=entry_count_color,
        legend="{}: Entry Count".format(team_size.name),
    )
    p.line(
        "date",
        "author_count_lowess",
        source=ColumnDataSource(team_size),
        y_range_name="team_range",
        line_width=2,
        color=author_count_color,
        legend="{}: Team Size".format(team_size.name),
    )
    return p


def _response_figure(p, issues, response_time, color_bar, color_plot):
    p.extra_y_ranges = {"response_range": Range1d(start=0, end=issues.shape[0])}

    p.vbar(
        x=issues["date"],
        top=issues["unanswered_to_this_date"],
        width=0.4,
        color=color_bar,
        y_range_name="response_range",
        legend="{}: Unanswered issues".format(response_time.name),
    )

    p.circle(
        "date",
        "response_time",
        source=ColumnDataSource(response_time),
        color=color_plot,
        fill_alpha=0.1,
        line_alpha=0.2,
    )

    p.line(
        "date",
        "response_time_lowess",
        source=ColumnDataSource(response_time),
        line_width=3,
        color=color_plot,
        legend="{}: Response time".format(response_time.name),
    )
    return p


def _display(objects, title, palette):
    obj_type = type(objects[0])
    # Displaying them all as if they're part of the same activity analysis... But is that what we want?
    if obj_type == Activity:
        df_to_display = pd.DataFrame()
        authors = []
        for obj in objects:
            df_to_display = pd.concat([df_to_display, obj.dataframe])
            authors += obj.authors
        p = figure(
            x_axis_type="datetime",
            y_range=list(set(authors)),
            sizing_mode="stretch_both",
            active_scroll="wheel_zoom",
            title=title,
        )
        p = _activity_figure(p, palette, df_to_display)
        return p

    # Displaying them all as if they're part of the same network analysis... But is that what we want?
    elif obj_type == Network:
        plots = []
        for obj in objects:
            p = figure(
                x_range=(-1.1, 1.1),
                y_range=(-1.1, 1.1),
                sizing_mode="stretch_both",
                active_scroll="wheel_zoom",
                title=title,
            )
            p = _network_figure(p, obj.dataframe, obj.graph)
            plots.append(p)
        # Do we really want this? Other forms of combining bokeh plots would not make much sense.
        gp = gridplot(plots, ncols=2, sizing_mode="stretch_both")
        # show(gp)
        return gp
    # Displaying each one in a different fig plot... Or should they be on the same one? (idem teamsize)
    elif obj_type == Response:
        p = figure(x_axis_type="datetime", sizing_mode="stretch_both", active_scroll="wheel_zoom", title=title)
        p.xaxis.axis_label = "Date"
        p.yaxis.axis_label = "Response Time (in hours)"

        p.add_layout(LinearAxis(y_range_name="response_range", axis_label="Response Rate"), "right")

        p.add_layout(Legend(), "below")

        p.add_tools(
            HoverTool(
                tooltips=[("Date", "@date{%Y-w%V}"), ("Response Time", "@response_time_formatted")],
                formatters={"date": "datetime"},
                point_policy="snap_to_data",
            )
        )
        color_i = 0
        df_num = 1
        for obj in objects:
            if "name" not in obj.response_time.__dict__:
                obj.response_time.name = "DataFrame #" + str(df_num)
            p = _response_figure(
                p, obj.unanswered_issues, obj.response_time, Category10[10][color_i], Category10[10][color_i + 1]
            )
            color_i += 2 if color_i < 8 else 0
            df_num += 1
        return p

    elif obj_type == TeamSize:
        author_count_max = max(obj.dataframe["author_count"].max() for obj in objects)
        p = figure(x_axis_type="datetime", sizing_mode="stretch_both", active_scroll="wheel_zoom", title=title)
        p.xaxis.axis_label = "Date"
        p.yaxis.axis_label = "Entry Count"

        p.extra_y_ranges = {"team_range": Range1d(start=0, end=author_count_max)}
        p.add_layout(LinearAxis(y_range_name="team_range", axis_label="Team Size"), "right")

        p.add_layout(Legend(), "below")

        p.add_tools(
            HoverTool(
                tooltips=[("Date", "@date{%Y-w%V}"), ("Team Size", "@author_count"), ("Entry Count", "@entry_count")],
                formatters={"date": "datetime"},
                point_policy="snap_to_data",
            )
        )
        color_i = 0
        for obj in objects:
            if "name" not in obj.dataframe.__dict__:
                obj.dataframe.name = "DataFrame #" + str(color_i + 1)
            p = _teamsize_figure(p, obj.dataframe, Category10[10][color_i], Category10[10][color_i + 1])
            color_i += 2 if color_i < 8 else 0
        return p

    elif obj_type == Centrality:
        p = figure(x_axis_type="datetime", sizing_mode="stretch_both", active_scroll="wheel_zoom")
        p.xaxis.axis_label = "Date"

        p.add_layout(Legend(), "below")

        p.add_tools(
            HoverTool(
                tooltips=[("Date", "@date{%Y-%m}"), ("Value", "@value{(0.000)}")],
                formatters={"date": "datetime"},
                mode="vline",
            )
        )
        color_i = 0
        for obj in objects:
            p = _centrality_figure(
                p, obj, Category10[10][color_i], Category10[10][color_i + 1], Category10[10][color_i + 2]
            )
            color_i += 3 if color_i < 7 else 0
        return p
