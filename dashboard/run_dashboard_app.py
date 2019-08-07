#!/usr/bin/env python3
"""Script that creates a dashboard website using the dash library.
The data comes from the specified dashboard db and
the information is updated every x-seconds (currently hardcoded to 10)
"""
import argparse
from collections import OrderedDict
from typing import List, Dict
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta

import numpy as np
import dash
import dash_html_components as html
import dash_core_components as dcc
import plotly.graph_objs as go
from dashboard.DashboardDB import DashboardDB, SQueueEntry, HPCProperty
from dashboard.run_data_collection import USERS
from dash.dependencies import Input, Output, State
import dash_table

import qcore.constants as const

# LAST_YEAR = datetime.strftime(datetime.now()-timedelta(days=365), "%y")
# CURRENT_YEAR = datetime.strftime(datetime.now(), "%y")
# ALLOCATION_TEMPLATE = ["01/06/{}-12:00:00", "01/12/{}-12:00:00"]
# ALLOCATIONS = [a.format(y) for y in [LAST_YEAR, CURRENT_YEAR] for a in ALLOCATION_TEMPLATE]

EXTERNAL_STYLESHEETS = [
    "https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css"
]

parser = argparse.ArgumentParser()
parser.add_argument("db_file", type=str, help="Path to the database file")
args = parser.parse_args()

# Creating the Dashboard app
app = dash.Dash(__name__, external_stylesheets=EXTERNAL_STYLESHEETS)
app.db = DashboardDB(args.db_file)


MAHUIKA_ALLOCATIONS = app.db.get_allocation_periods(const.HPC.mahuika)
MAUI_ALLOCATIONS = app.db.get_allocation_periods(const.HPC.maui)


ALLOCATIONS_MAHUIKA = ["{}---{}".format(i[0], i[1]) for i in MAHUIKA_ALLOCATIONS]
ALLOCATIONS_MAUI = ["{}---{}".format(i[0], i[1]) for i in MAUI_ALLOCATIONS]

app.layout = html.Div(
    html.Div(
        [
            # 1
            html.H2("Maui & Mahuika"),
            html.Div(id="err"),
            html.Div(id="err2"),
            html.Div(id="err3"),
            # 2
            html.H4("Maui & Mahuika total core hours usage"),
            html.Div(id="maui_chours"),
            html.Div(id="mahuika_chours"),
            # 3
            html.H4("Maui Allocation"),
            dcc.Dropdown(
                id="maui-dropdown",
                options=[{"label": i, "value": i} for i in ALLOCATIONS_MAUI],
                value=ALLOCATIONS_MAUI[-1],
                clearable=False,
            ),
            html.H5("Maui allocation start date:", style={"padding-top": 25}),
            dcc.DatePickerSingle(
                id="maui-input-start",
                min_date_allowed=MAUI_ALLOCATIONS[0][0],
                max_date_allowed=datetime.now().date(),
                initial_visible_month=MAUI_ALLOCATIONS[0][0],
                display_format="YYYY-MM-DD",
                clearable=True,
            ),
            html.H5("Maui allocation end date:"),
            dcc.DatePickerSingle(
                id="maui-input-end",
                min_date_allowed=MAHUIKA_ALLOCATIONS[0][0],
                max_date_allowed=datetime.now().date(),
                initial_visible_month=MAHUIKA_ALLOCATIONS[0][0],
                display_format="YYYY-MM-DD",
                clearable=True,
            ),
            html.H5("Maui total core hour usage", style={"padding-top": 25}),
            dcc.Graph(id="maui_total_chours"),
            html.H5("Maui daily core hour usage", style={"padding-top": 25}),
            dcc.Graph(id="maui_daily_chours"),
            # 4
            html.H5("Maui total user core hours"),
            html.Div(id="maui_total_user_chours"),
            # 5
            html.H4("Mahuika Allocation", style={"padding-top": 30}),
            dcc.Dropdown(
                id="mahuika-dropdown",
                options=[{"label": i, "value": i} for i in ALLOCATIONS_MAHUIKA],
                value=ALLOCATIONS_MAHUIKA[-1],
                clearable=False,
            ),
            html.H5("Mahuika allocation start date:", style={"padding-top": 25}),
            dcc.DatePickerSingle(
                id="mahuika-input-start",
                min_date_allowed=MAHUIKA_ALLOCATIONS[0][0],
                max_date_allowed=datetime.now().date(),
                initial_visible_month=MAHUIKA_ALLOCATIONS[0][0],
                display_format="YYYY-MM-DD",
                clearable=True,
            ),
            html.H5("Mahuika allocation end date:"),
            dcc.DatePickerSingle(
                id="mahuika-input-end",
                min_date_allowed=MAHUIKA_ALLOCATIONS[0][0],
                max_date_allowed=datetime.now().date(),
                initial_visible_month=MAHUIKA_ALLOCATIONS[0][0],
                display_format="YYYY-MM-DD",
                clearable=True,
            ),
            html.H5("Mahuika total core hour usage", style={"padding-top": 25}),
            dcc.Graph(id="mahuika_total_chours"),
            # 6
            html.H5("Mahuika daily core hour usage", style={"padding-top": 30}),
            dcc.Graph(id="mahuika_daily_chours"),
            # 7
            html.H5("Mahuika total user core hours"),
            html.Div(id="mahuika_total_user_chours"),
            # 8
            html.H5("Maui current quota", style={"padding-top": 25}),
            html.Div(id="maui_quota_usage"),
            # 9
            html.H5("Maui_daily_inodes", style={"padding-top": 25}),
            dcc.Graph(id="maui_daily_inodes"),
            # 10
            html.H5("Maui current status"),
            html.Div(id="maui_node_usage"),
            # 11
            html.H5("Maui current queue"),
            html.Div(id="maui_squeue_table"),
            # Update interval
            dcc.Interval(id="interval_comp", interval=600 * 1000, n_intervals=0),
        ]
    )
)


@app.callback(
    Output("err2", "children"),
    [Input("maui-input-start", "date"), Input("maui-input-end", "date")],
)
def display_err_maui(input_start, input_end):
    output, _, _ = get_allocation_period(MAUI_ALLOCATIONS, input_start, input_end)
    return output


@app.callback(
    Output("err3", "children"),
    [Input("mahuika-input-start", "date"), Input("mahuika-input-end", "date")],
)
def display_err_mahuika(input_start, input_end):
    output, _, _ = get_allocation_period(MAHUIKA_ALLOCATIONS, input_start, input_end)
    return output


@app.callback(
    Output("maui_chours", "children"),
    [Input("maui-input-start", "date"), Input("maui-input-end", "date")],
)
def update_maui_total_chours(input_start, input_end):
    return update_total_chours(MAUI_ALLOCATIONS, input_start, input_end, const.HPC.maui)


@app.callback(
    Output("mahuika_chours", "children"),
    [Input("mahuika-input-start", "date"), Input("mahuika-input-end", "date")],
)
def update_mahuika_total_chours(input_start, input_end):
    return update_total_chours(
        MAHUIKA_ALLOCATIONS, input_start, input_end, const.HPC.mahuika
    )


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
    Output("maui_daily_chours", "figure"),
    [Input("maui-input-start", "date"), Input("maui-input-end", "date")],
)
def update_maui_daily_chours(input_start, input_end):
    _, start_date, end_date = get_allocation_period(
        MAUI_ALLOCATIONS, input_start, input_end
    )
    return update_daily_chours(const.HPC.maui, start_date, end_date)


@app.callback(
    [Output("mahuika-input-start", "date"), Output("mahuika-input-end", "date")],
    [Input("mahuika-dropdown", "value")],
)
def update_mahuika_datepicker(drop_down_value):
    return drop_down_value.split("---")


@app.callback(
    [Output("maui-input-start", "date"), Output("maui-input-end", "date")],
    [Input("maui-dropdown", "value")],
)
def update_maui_datepicker(drop_down_value):
    return drop_down_value.split("---")


@app.callback(
    Output("mahuika_daily_chours", "figure"),
    [Input("mahuika-input-start", "date"), Input("mahuika-input-end", "date")],
)
def update_mahuika_daily_chours(input_start, input_end):
    _, start_date, end_date = get_allocation_period(
        MAHUIKA_ALLOCATIONS, input_start, input_end
    )
    return update_daily_chours(const.HPC.mahuika, start_date, end_date)


@app.callback(
    Output("maui_total_chours", "figure"),
    [Input("maui-input-start", "date"), Input("maui-input-end", "date")],
)
def update_maui_total_chours(input_start, input_end):
    _, start_date, end_date = get_allocation_period(
        MAUI_ALLOCATIONS, input_start, input_end
    )
    entries = get_chours_entries(const.HPC.maui, start_date, end_date)
    fig = go.Figure()
    fig.add_scatter(x=entries["day"], y=entries["total_chours"])

    return fig


@app.callback(
    Output("mahuika_total_chours", "figure"),
    [Input("mahuika-input-start", "date"), Input("mahuika-input-end", "date")],
)
def update_mahuika_total_chours(input_start, input_end):
    _, start_date, end_date = get_allocation_period(
        MAHUIKA_ALLOCATIONS, input_start, input_end
    )
    entries = get_chours_entries(const.HPC.mahuika, start_date, end_date)
    fig = go.Figure()
    fig.add_scatter(x=entries["day"], y=entries["total_chours"])

    return fig


@app.callback(
    Output("mahuika_total_user_chours", "children"),
    [Input("mahuika-input-start", "date"), Input("mahuika-input-end", "date")],
)
def update_mahuika_total_user_chours(input_start, input_end):
    _, start_date, end_date = get_allocation_period(
        MAHUIKA_ALLOCATIONS, input_start, input_end
    )
    return get_total_user_chours(const.HPC.mahuika, USERS, start_date, end_date)


@app.callback(
    Output("maui_total_user_chours", "children"),
    [Input("maui-input-start", "date"), Input("maui-input-end", "date")],
)
def update_maui_total_user_chours(input_start, input_end):
    _, start_date, end_date = get_allocation_period(
        MAUI_ALLOCATIONS, input_start, input_end
    )
    return get_total_user_chours(const.HPC.maui, USERS, start_date, end_date)


@app.callback(Output("err", "children"), [Input("interval_comp", "n_intervals")])
def display_err(n):
    """Displays data collection error when the gap between update_times exceeds acceptable limit"""
    if not check_update_time(app.db.get_update_time(const.HPC.maui)[0], datetime.now()):
        return html.Plaintext(
            "Data collection error, check the error_table in database",
            style={"background-color": "red", "font-size": 20},
        )


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
    data = []
    trace = go.Scatter(
        x=entries["day"], y=entries["used_inodes"], name="maui_nobackup_daily_inodes"
    )
    data.append(trace)
    # max available inodes on maui nobackup
    trace2 = go.Scatter(
        x=entries["day"],
        y=np.tile(15000000, entries["used_inodes"].size),
        name="maui_nobackup_available inodes",
        fillcolor="red",
    )
    data.append(trace2)
    layout = go.Layout(yaxis=dict(range=[0, 16000000]))
    fig = go.Figure(data=data, layout=layout)
    return fig


def update_daily_chours(hpc, start_date=None, end_date=None):
    # Get data points
    data = []
    entries = app.db.get_chours_usage(start_date, end_date, hpc)
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
    data += get_daily_user_chours(hpc, USERS, start_date, end_date)

    # uirevision preserve the UI state between update intervals
    return {"data": data, "layout": {"uirevision": "{}_daily_chours".format(hpc)}}


def get_daily_user_chours(
    hpc: const.HPC, users_dict: Dict[str, str] = USERS, start_date=None, end_date=None
):
    """get daily core hours usage for a list of users
       return as a list of scatter plots
    """
    data = []
    for username, real_name in users_dict.items():
        entries = app.db.get_user_chours(hpc, username, start_date, end_date)
        entries = np.array(
            entries,
            dtype=[
                ("day", "datetime64[D]"),
                ("username", object),
                ("core_hours_used", float),
            ],
        )
        trace = go.Scatter(
            x=entries["day"], y=entries["core_hours_used"], name=real_name
        )
        data.append(trace)
    return data


def get_total_user_chours(
    hpc: const.HPC, users_dict: Dict[str, str] = USERS, start_date=None, end_date=None
):
    """Get total core hours usage for a list of users in a specified period
       Return as a table
    """
    data = []
    for username, real_name in users_dict.items():
        name, total_chours = app.db.get_total_user_chours(
            hpc, username, start_date, end_date
        )
        data.append({"username": USERS[name], "total_core_hours": total_chours})

    # first sort by decs total core hours
    data = sorted(data, key=lambda k: k["total_core_hours"], reverse=True)

    # then add comma to big values
    for i in range(len(data)):
        data[i]["total_core_hours"] = "{:,}".format(data[i]["total_core_hours"])

    return html.Div(
        [
            dash_table.DataTable(
                id="table",
                columns=[
                    {"id": c, "name": c} for c in ["username", "total_core_hours"]
                ],
                data=data,
                style_cell={"textAlign": "left"},
            )
        ],
        id="table_container",
        style={"float": "centre", "width": "50%", "padding-left": 50},
    )


def get_chours_entries(hpc: const.HPC, start_date=None, end_date=None):
    """Gets the core hours entries for the specified HPC
    Note: Only maui is currently supported
    """
    # Get data points
    entries = app.db.get_chours_usage(start_date, end_date, hpc)
    if entries:
        # reset start of a new allocation to 0 hours
        start_total = entries[0][-1]
        entries = [(*entry[:2], entry[2] - start_total) for entry in entries]

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
    return html.Div(
        [
            dash_table.DataTable(
                id="datatable-interactivity",
                columns=[
                    {"name": i, "id": i, "deletable": False}
                    for i in SQueueEntry._fields
                ],
                data=squeue_entries,
                filtering=True,
                filtering_settings="account eq 'nesi00213'",
                sorting=True,
                sorting_type="multi",
            ),
            html.Div(id="datatable-interactivity-container"),
        ]
    )


def get_maui_daily_quota_string(file_system):
    """Get daily quota string for a particular file system eg.nobackup"""
    entry = app.db.get_daily_quota(
        const.HPC.maui, date.today(), file_system=file_system
    )
    return "Current space usage in {} is {}\nCurrent Inodes usage in {} is {:,}/{:,} ({:.1f}%)".format(
        file_system,
        entry.used_space,
        file_system,
        entry.used_inodes,
        entry.available_inodes,
        entry.used_inodes / entry.available_inodes * 100.0,
    )


def check_update_time(last_update_time_string: str, current_update_time: datetime):
    """Checks whether the time gap between update times exceeds the idling time limit(1500s)
    if exceeds, regards as a collection error.
    """
    # 2019-03-28 18:31:11.906576
    return (
        current_update_time
        - datetime.strptime(last_update_time_string, "%Y-%m-%d %H:%M:%S.%f")
    ) < timedelta(seconds=1500)


def validate_period(start_string, end_string):
    """
    Validates user input date strings for custom allocation period
    Returns either error message or datetime objects
    """
    try:
        start = datetime.strptime(start_string, "%Y-%m-%d").date()
        end = datetime.strptime(end_string, "%Y-%m-%d").date()
    except ValueError:
        return html.Plaintext(
            "Date not in the correct format specified, setting dates to the latest period",
            style={"background-color": "red", "font-size": 20},
        )
    if start > end:
        return html.Plaintext(
            "End date must be after start date, setting dates to the latest period",
            style={"background-color": "red", "font-size": 20},
        )
    return start, end


def get_allocation_period(default_allocations, input_start, input_end):
    """
    Gets input allocation period either from text box or dropdown
    Returns error message and datetime objects.
    """
    if input_start and input_end:
        output = validate_period(input_start, input_end)
        if not isinstance(
            output, tuple
        ):  # returned err msg, setting allocation period to defaults
            start_date, end_date = default_allocations[-1]
            return output, start_date, end_date
        else:
            start_date, end_date = output
    else:
        start_date, end_date = default_allocations[-1]
    return None, start_date, end_date  # None: no err placeholder


def update_total_chours(default_allocations, input_start, input_end, hpc):
    """Updates total core hours for a specified hpc during a specified allocation period"""
    _, start_date, end_date = get_allocation_period(
        default_allocations, input_start, input_end
    )
    hpc_total_chours = get_chours_entries(hpc, start_date, end_date)[-1][-1]
    max_hours_result = app.db.get_allocation_hours(hpc, input_start, input_end)
    if max_hours_result:
        max_hours = max_hours_result[0]
        usage = hpc_total_chours / max_hours * 100.0
        template = "{}: {} to {} used {:,.1f} / {:,.1f} hours ({:.1f}%)"
    # custom allocation period entered, cannot decide the max core hours due to overlapped allocation periods
    else:
        max_hours = " "
        usage = " "
        template = "{}: {} to {} used {:,.1f} hours{}{}"
    return html.Plaintext(
        template.format(
            hpc.value, start_date, end_date, hpc_total_chours, max_hours, usage
        )
    )


if __name__ == "__main__":
    app.run_server(host="0.0.0.0")
