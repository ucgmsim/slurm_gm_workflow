#!/usr/bin/env python3
"""Script that creates a dashboard website using the dash library.
The data comes from the specified dashboard db and
the information is updated every x-seconds (currently hardcoded to 10)
"""
import argparse
from typing import List
from datetime import date
from dateutil.relativedelta import relativedelta

import numpy as np
import dash
import dash_html_components as html
import dash_core_components as dcc
import plotly.graph_objs as go
from dashboard.DashboardDB import DashboardDB, SQueueEntry, HPCProperty
from dash.dependencies import Input, Output

import qcore.constants as const

EXTERNAL_STYLESHEETS = ["https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css"]

parser = argparse.ArgumentParser()
parser.add_argument("db_file", type=str, help="Path to the database file")
args = parser.parse_args()

# Creating the Dashboard app
app = dash.Dash(__name__, external_stylesheets=EXTERNAL_STYLESHEETS)
app.layout = html.Div(
    html.Div(
        [
            html.H3("Maui"),
            html.Div(
                [
                    html.H5("Current status"),
                    html.Div(id="maui_node_usage"),
                    html.H5("Current queue"),
                    html.Div(id="maui_squeue_table"),
                    html.H5("Daily core hour usage"),
                    dcc.Graph(id="maui_daily_chours"),
                    html.H5("Total core hour usage"),
                    dcc.Graph(id="maui_total_chours"),
                    dcc.Interval(id="interval_comp", interval=10 * 1000, n_intervals=0),
                ]
            ),
        ]
    )
)

app.db = DashboardDB(args.db_file)


@app.callback(
    Output("maui_node_usage", "children"), [Input("interval_comp", "n_intervals")]
)
def update_maui_nodes(n):
    entry = app.db.get_status_entry(const.HPC.maui, HPCProperty.node_capacity.value)
    return html.Plaintext(
        "Current number of nodes available {}/{}".format(
            entry.int_value_2 - entry.int_value_1, entry.int_value_2
        )
    )


@app.callback(
    Output("maui_squeue_table", "children"), [Input("interval_comp", "n_intervals")]
)
def update_maui_squeue(n):
    entries = app.db.get_squeue_entries(const.HPC.maui)
    return generate_table(entries)


@app.callback(
    Output("maui_daily_chours", "figure"), [Input("interval_comp", "n_intervals")]
)
def update_maui_daily_chours(n):
    # Get data points
    entries = app.db.get_chours_usage(
        date.today() - relativedelta(years=1), date.today(), const.HPC.maui
    )
    entries = np.array(
        entries,
        dtype=[
            ("day", "datetime64[D]"),
            ("daily_chours", float),
            ("total_chours", float),
        ],
    )

    fig = go.Figure()
    fig.add_scatter(x=entries["day"], y=entries["daily_chours"])

    return fig


@app.callback(
    Output("maui_total_chours", "figure"), [Input("interval_comp", "n_intervals")]
)
def update_maui_total_chours(n):
    entries = get_chours_entries(const.HPC.maui)

    fig = go.Figure()
    fig.add_scatter(x=entries["day"], y=entries["total_chours"])

    return fig


def get_chours_entries(hpc: const.HPC):
    """Gets the core hours entries for the specified HPC
    Note: Only maui is currently supported
    """
    # Get data points
    entries = app.db.get_chours_usage(
        date.today() - relativedelta(years=1), date.today(), const.HPC.maui
    )
    return np.array(
        entries,
        dtype=[
            ("day", "datetime64[D]"),
            ("daily_chours", float),
            ("total_chours", float),
        ],
    )


def generate_table(squeue_entries: List[SQueueEntry]):
    """Generates html table for the given squeue entries.
    """
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in SQueueEntry._fields])]
        +
        # Body
        [html.Tr([html.Td(col_val) for col_val in entry]) for entry in squeue_entries]
    )


if __name__ == "__main__":
    app.run_server(host="0.0.0.0")
