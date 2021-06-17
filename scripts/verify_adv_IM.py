"""script to quickly check the status of adv_im for a event
checks for existance of aggregated csv
checks for nan/null in the csv
checks station in csv matches stations ran
"""


import argparse
from datetime import datetime
from enum import Enum
import os
import sys

import pandas as pd

from qcore import constants as const
from qcore.formats import load_station_file
from IM_calculation.Advanced_IM.runlibs_2d import check_status, TIME_FORMAT, time_type


class run_status(Enum):
    not_started = 0
    finished = 1
    not_converged = 2
    not_finished = 3
    crashed = 4
    unknown = 5


column_names = [
    "station",
    "model",
    "component",
    "status",
    time_type.start_time.name,
    time_type.end_time.name,
]


def read_timelog(comp_run_dir):
    t_list = [None for y in time_type]
    for t_type in time_type:
        path_logfile = os.path.join(comp_run_dir, t_type.name)
        if not os.path.isfile(path_logfile):
            continue
        with open(path_logfile, "r") as f:
            time = datetime.strptime(f.readline(), TIME_FORMAT)
            t_list[t_type.value] = time
    return t_list


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "im_calc_dir", type=str, help="The path to the realisation directory"
    )
    parser.add_argument(
        "adv_im_model", nargs="+", type=str, help="list of adv_IM models that ran"
    )

    parser.add_argument(
        "--components",
        default=[const.Components.c000.str_value, const.Components.c090.str_value],
        nargs="+",
        choices=list(const.Components.iterate_str_values()),
        help="list of component that ran",
    )

    parser.add_argument(
        "--station_file",
        default=None,
        type=str,
        help="if set, script will only check for folder-names that match the station file",
    )

    # saves output
    parser.add_argument(
        "--save_status",
        default=None,
        #        action="store_true",
        type=str,
        help="will override the 'skip' behavior",
    )

    args = parser.parse_args()

    return args


def check_log(list_folders, model, components, df_model, break_on_fail=False):
    """
    check of "Failed" msg in logs
    no "Failed" in all Analysis*.txt == crashed
    "Failed" in all Analysis*.txt == failed to converge (normal)
    """
    #    print(f"{df_model}")
    for station_dir in list_folders:
        # check if folder is a station run folder
        station_model_dir = os.path.join(station_dir, model)
        station_name = os.path.basename(station_dir)
        for comp in components:
            component_outdir = os.path.join(station_model_dir, comp)
            time_list = read_timelog(component_outdir)
            if check_status(component_outdir):
                # successed
                station_component_status = run_status.finished.value
                test = read_timelog(component_outdir)
                # update df and scan for datetime
            # check for 'Failed' keyword
            elif check_status(component_outdir, check_fail=True):
                # all logs showed "Failed", analysis was unable to converge
                station_component_status = run_status.not_converged.value
            elif time_list.count(None) == 0:
                # if start and end are there, but no data = crashed
                station_component_status = run_status.crashed.value
            elif time_list.count(None) == 1:
                # only starttime exist = wct timed out
                station_component_status = run_status.not_finished.value
                # else not started
            comp_mask = (df_model["station"] == station_name) & (
                df_model["component"] == comp
            )
            df_model.loc[comp_mask, "status"] = station_component_status
            df_model.loc[comp_mask, time_type.start_time.name] = time_list[
                time_type.start_time.value
            ]
            df_model.loc[comp_mask, time_type.end_time.name] = time_list[
                time_type.end_time.value
            ]
    #    print(f"{df_model}")
    return df_model


def main(im_calc_dir, adv_im_model, components, save_status=False, station_file=None):

    #    df_station_status = pd.DataFrame(columns = column_names)
    df_dict = {}
    for model in adv_im_model:
        csv_path = os.path.join(im_calc_dir, "{}.csv".format(model))
        status_csv_path = os.path.join(im_calc_dir, "{}_status.csv".format(model))

        if station_file is not None:
            station_list = load_station_file(station_file).index.tolist()
        else:
            # glob for station folders
            # station_list = [ y for y in [os.path.join(im_calc_dir, x) for x in os.listdir(im_calc_dir)] if os.path.isdir(y) ]
            station_list = [x for x in os.listdir(im_calc_dir) if os.path.isdir(x)]
        list_folders = [os.path.join(im_calc_dir, x) for x in station_list]

        # initialize df with empty value with not started
        df_model = pd.DataFrame(columns=column_names)
        for component in components:
            df_model = pd.concat(
                [
                    df_model,
                    pd.DataFrame(
                        {
                            "station": station_list,
                            "model": model,
                            "component": component,
                            "status": run_status.not_started.value,
                        },
                        columns=column_names,
                    ),
                ],
                ignore_index=True,
            )
        #            df_model = pd.concat([df_model,df_test],ignore_index=True)
        #            print(f"{df_model}")

        # a quick check to compare station count, will skip all other checks if successful.
        # skip this step if 'save_status' is set
        if not bool(save_status):
            # using try/except to prevent crash
            try:
                df_csv = pd.read_csv(csv_path)
            except FileNotFoundError:
                # failed to read a agg csv, leave df_model as is.
                print("csv for {} does not exist".format(model))
                continue
            # check for null/nan
            if df_csv.isnull().values.any():
                # agg csv is there, but value has errors
                # change all status to unknown
                df_model.loc["status"] = run_status.unknown.value
                continue

            # check station count, if match, skip rest of checks
            csv_stations = df_csv.station.unique()
            if len(csv_stations) == len(station_list):
                # station folder count matches csv.station.count
                df_model.loc["status"] = run_status.finished.value
                continue
            # not matched, will continue rest of the test to scan logs for each station
        # check for logs
        check_log(list_folders, model, components, df_model, break_on_fail=True)
        df_dict[model] = df_model

        # sort index by status
        #        df_model['sort'] = df[]
        df_model.sort_values("status", inplace=True, ascending=False)
        print(f"{df_model}")
        # map status(int) to string before saving as csv
        df_model["status"] = df_model["status"].map(lambda x: run_status(x).name)
        #        df_model.set_index(pd.Index)
        print(f"{df_model}")
        df_model.to_csv(status_csv_path, header=True, index=True)


#        df_station_status = pd.concat([df_station_status,df_model],ignore_index=True)
#        print(f"{df_station_status}")


#        if len(failed_stations) != 0:
#            failed.append((model, "failed to converge", failed_stations))
#            failed_msg = f"some stations failed to converge {failed}"
#        if len(crashed_stations) != 0:
#            crashed.append((model, "crashed", crashed_stations))

#    print(f"{df_station_status}")
# save df if option set
#    if save_status:
#        out_file = save_status
#    else:
#        out_file = os.path.join(im_calc_dir,'station_status.csv')

# or print out all status > 2

#    time_type[df.status].value > time_type.finished.value
#
#    if len(crashed) != 0:
#        print(f"some runs have crashed. models: {crashed}")
#        return 1
#    else:
#        print(f"check passed. {failed_msg}")
#        return 0


if __name__ == "__main__":
    args = parse_args()
    res = main(
        args.im_calc_dir,
        args.adv_im_model,
        args.components,
        save_status=args.save_status,
        station_file=args.station_file,
    )
    sys.exit(res)
