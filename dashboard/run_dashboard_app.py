#!/usr/bin/env python3
"""Script that creates a dashboard website using the dash library.
The data comes from the specified dashboard db and
the information is updated every x-seconds (currently hardcoded to 10)
"""
import argparse
import json
from typing import List, Dict
from datetime import date
from dateutil.relativedelta import relativedelta

import numpy as np
import dash
import dash_html_components as html
import dash_core_components as dcc
import plotly.graph_objs as go
from dashboard.DashboardDB import DashboardDB, SQueueEntry, HPCProperty
from dashboard.run_data_collection import USERS
from dash.dependencies import Input, Output
import dash_table

import qcore.constants as const

EXTERNAL_STYLESHEETS = [
    "https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css"
]

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
                    html.Div([
                        dcc.ConfirmDialog(
                            id='confirm',
                            message='Collection error! Check the database error table!',
                        ),
                        html.Div(id='output-confirm')
                    ]),

                    html.H5("Current status"),
                    html.Div(id="maui_node_usage"),
                    html.H5("Current quota"),
                    html.Div(id="maui_quota_usage"),
                    html.H5("Current queue"),
                    html.Div(id="maui_squeue_table"),
                    html.H5("Daily core hour usage"),
                    dcc.Graph(id="maui_daily_chours"),
                    html.H5("Total core hour usage"),
                    dcc.Graph(id="maui_total_chours"),
                    dcc.Interval(id="interval_comp", interval=10 * 1000, n_intervals=0),
                    html.H5("maui_daily_inodes"),
                    dcc.Graph(id="maui_daily_inodes"),
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
    Output("maui_quota_usage", "children"), [Input("interval_comp", "n_intervals")]
)
def update_maui_quota(n):
    nobackup_string = get_maui_daily_quota_string("nobackup")
    project_string = get_maui_daily_quota_string("project")
    return html.Plaintext("{}\n{}".format(nobackup_string, project_string))


@app.callback(
    Output("maui_squeue_table", "children"), [Input("interval_comp", "n_intervals")]
)
def update_maui_squeue(n):
    entries = app.db.get_squeue_entries(const.HPC.maui)
    return generate_table_interactive(entries)


@app.callback(
    Output("maui_daily_chours", "figure"), [Input("interval_comp", "n_intervals")]
)
def update_maui_daily_chours(n):
    # Get data points
    data = []
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
    trace = go.Scatter(x=entries["day"], y=entries["daily_chours"], name="daily_chours")
    data.append(trace)

    # get core hours usage for each user
    data += get_maui_daily_user_chours(const.HPC.maui, USERS)

    # uirevision preserve the UI state between update intervals
    return {'data': data, 'layout': {'uirevision': "maui_daily_chours"}}


@app.callback(
    Output("maui_total_chours", "figure"), [Input("interval_comp", "n_intervals")]
)
def update_maui_total_chours(n):
    entries = get_chours_entries(const.HPC.maui)

    fig = go.Figure()
    fig.add_scatter(x=entries["day"], y=entries["total_chours"])

    return fig


@app.callback(Output('confirm', 'displayed'),
              [Input("interval_comp", "n_intervals")])
def display_confirm(n):
    return app.db.get_collection_err(const.HPC.maui) is not None


@app.callback(Output('output-confirm', 'children'),
              [Input('confirm', 'submit_n_clicks')])
def update_output(submit_n_clicks):
    if submit_n_clicks:
        return None


@app.callback(
    Output("maui_daily_inodes", "figure"), [Input("interval_comp", "n_intervals")]
)
def update_maui_daily_inodes(n):
    entries = app.db.get_daily_inodes(const.HPC.maui)

    entries = np.array(
        entries,
        dtype=[
            ("file_system", object),
            ("used_inodes", float),
            ("day", "datetime64[D]"),
        ],
    )
    fig = go.Figure()
    fig.layout.title = entries["file_system"][0]
    fig.add_scatter(x=entries["day"], y=entries["used_inodes"])

    return fig


def get_maui_daily_user_chours(hpc: const.HPC, users_dict: Dict[str, str]=USERS):
    """get daily core hours usage for a list of users
       return as a list of scatter plots
    """
    data = []
    for username, real_name in users_dict.items():
        entries = app.db.get_user_chours(hpc, username)
        entries = np.array(entries, dtype=[
            ("day", "datetime64[D]"),
            ("username", object),
            ("core_hours_used", float)])
        trace = go.Scatter(x=entries["day"], y=entries["core_hours_used"], name=real_name)
        data.append(trace)
    return data


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
    """Generates html table for the given squeue entries."""
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in SQueueEntry._fields])]
        +
        # Body
        [html.Tr([html.Td(col_val) for col_val in entry]) for entry in squeue_entries]
    )


def generate_table_interactive(squeue_entries: List[SQueueEntry]):
    """Generates interactive dash table for the given squeue entries."""
    # Convert NamedTuple to OrderedDict
    squeue_entries = [entry._asdict() for entry in squeue_entries]
    return html.Div([
        dash_table.DataTable(
            id='datatable-interactivity',
            columns=[
                {"name": i, "id": i, "deletable": False} for i in SQueueEntry._fields
            ],
            data=squeue_entries,
            filtering=True,
            filtering_settings="account eq 'nesi00213'",
            sorting=True,
            sorting_type="multi",
            pagination_mode="fe",
            pagination_settings={
                "displayed_pages": 1,
                "current_page": 0,
                "page_size": 35,
            },
            navigation="page",
        ),
        html.Div(id='datatable-interactivity-container')
    ])


def get_maui_daily_quota_string(file_system):
    """Get daily quota string for a particular file system eg.nobackup"""
    entry = app.db.get_daily_quota(
        const.HPC.maui, date.today(), file_system=file_system
    )
    return "Current space usage in {} is {}\nCurrent Inodes usage in {} is {}/{} ({:.3f}%)".format(
        file_system,
        entry.used_space,
        file_system,
        entry.used_inodes,
        entry.available_inodes,
        entry.used_inodes / entry.available_inodes * 100.
    )


if __name__ == "__main__":
    app.run_server(host="0.0.0.0")
