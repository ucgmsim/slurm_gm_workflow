#!/usr/bin/env python3
"""Reads metadata json files created by write_json script and combines
the data into a single dataframe, which is then saved as a .csv

Searches recursively for the .json files for a given start dir.

Note: This only works in python3, python2.7 glob does not support
recursive directory traversal.
"""
import ast
import sys
import os
import glob
import json
import numpy as np
import pandas as pd

from enum import Enum
from multiprocessing import Pool
from argparse import ArgumentParser

from estimation.estimate_wct import get_IM_comp_count

DATE_COLUMNS = ["end_time", "start_time", "submit_time"]
DATETIME_FMT = "%Y-%m-%d_%H:%M:%S"


class MetaConst(Enum):
    run_time = "run_time"
    core_hours = "core_hours"
    n_cores = "cores"
    fd_count = "fd_count"
    nsub_stoch = "nsub_stoch"
    dt = "dt"
    nt = "nt"
    nx = "nx"
    ny = "ny"
    nz = "nz"
    start_time = "start_time"
    end_time = "end_time"

    im_pSA_count = "pSA_count"
    im_comp = "im_components"
    im_comp_count = "im_components_count"


class ProcTypeConst(Enum):
    BB = "BB"
    HF = "HF"
    LF = "LF"
    IM = "IM_calc"
    POST_EMOD3D = "POST_EMOD3D"

    @classmethod
    def has_value(cls, value):
        return any(value == item.value for item in cls)


def create_dataframe(json_file):
    """Create a dataframe for the given json file"""
    with open(json_file) as f:
        data_dict = json.load(f)

    index = []
    columns = []

    # Get all the column paths (into the json)
    # Have to do this in advance before actually reading the data
    # as these can vary between realisation
    for rel_key in data_dict.keys():
        index.append(rel_key)

        # LF/HF/BB
        cur_columns = []
        for run_type_key in data_dict[rel_key].keys():
            for val_key in data_dict[rel_key][run_type_key].keys():
                cur_columns.append((run_type_key, val_key))

        if len(cur_columns) > len(columns):
            columns = cur_columns

    if len(columns) > 0:
        row_data = get_row_data(data_dict, columns)

        # Remove empty rows
        empty_rows_mask = np.all(pd.isnull(row_data), axis=1)
        row_data = row_data[~empty_rows_mask]
        index = np.asarray(index)[~empty_rows_mask]

        df = pd.DataFrame(
            index=index, columns=pd.MultiIndex.from_tuples(columns), data=row_data
        )
    else:
        print("The metadata file {} is empty. Skipping.".format(json_file))
        return None

    # Tidy up the dataframe
    df = clean_df(df)

    return df


def get_row_data(data_dict, column_paths):
    """Gets the row data for a specific realisation.

    If entries are missing then these are filled out with np.nan
    """
    row_data = []
    for rel_key in data_dict.keys():
        cur_row_data = []
        for sim_key, val_key in column_paths:
            # Check that the sim type (HF, BB, LF, IM_calc) exists for this realisation
            if sim_key in data_dict[rel_key].keys():
                # Check that this value key exists and save
                if val_key in data_dict[rel_key][sim_key]:
                    cur_row_data.append(data_dict[rel_key][sim_key][val_key])
                    continue

            # Otherwise add None
            cur_row_data.append(np.nan)

        row_data.append(cur_row_data)
    return np.asarray(row_data)


def clean_df(df):
    """Cleans column of interests,
    and attempts to convert columns to numeric data type (float)"""
    # Iterate BB, HF, LF
    for sim_type_const in ProcTypeConst:
        sim_type = sim_type_const.value

        if sim_type in df.columns.levels[0].values:
            # All available metadata
            for meta_col in df[sim_type].columns.values:
                # Run time, remove "hour"
                if MetaConst.run_time.value == meta_col:
                    rt_param = MetaConst.run_time.value

                    # Convert column type to float
                    df[sim_type, rt_param] = np.asarray(
                        [
                            (
                                value.split(" ")[0] if type(value) is str else value
                            )  # Handle np.nan values
                            for value in df[sim_type, rt_param].values
                        ],
                        dtype=np.float32,
                    )
                # Count the number of components calculated
                elif meta_col == MetaConst.im_comp.value:
                    df.loc[:, (sim_type, MetaConst.im_comp_count.value)] = [
                        get_IM_comp_count_from_str(str_list, real)
                        for real, str_list in df.loc[
                            :, (sim_type, meta_col)
                        ].iteritems()
                    ]
                # Convert date strings to date type
                elif meta_col in DATE_COLUMNS:
                    df[sim_type, meta_col] = pd.to_datetime(
                        df[sim_type, meta_col], format=DATETIME_FMT, errors="coerce"
                    )
                # Try to convert everything else to numeric
                else:
                    df[sim_type, meta_col] = pd.to_numeric(
                        df[sim_type, meta_col], errors="coerce", downcast="float"
                    )

    return df


def get_IM_comp_count_from_str(str_list: str, real_name: str):
    """Gets the IM component count, see get_IM_comp_count for better doc"""
    try:
        comp = ast.literal_eval(str_list)
    except ValueError:
        print("Failed to determine number of components for {}".format(real_name))
        return np.nan

    return get_IM_comp_count(comp)


def add_im_comp_count(df):
    """Special handling for the im components column.
    Counts the components and adds a new column for component count."""

    return df


def main(args):
    # Check if the output file already exists (No overwrite)
    if os.path.isfile(args.output_file):
        print("Output file already exists. Not proceeding. Exiting.")
        sys.exit()

    # Get all .json files
    print("Searching for matching json files")
    file_pattern = (
        "{}.json".format(args.filename_pattern)
        if args.not_recursive
        else os.path.join("**/", "{}.json".format(args.filename_pattern))
    )

    json_files = [
        glob.glob(os.path.join(cur_dir, file_pattern), recursive=not args.not_recursive)
        for cur_dir in args.input_dirs
    ]

    # Flatten the list of list of files
    json_files = [file for file_list in json_files for file in file_list]

    if len(json_files) == 0:
        print("No matching .json files found. Quitting.")
        sys.exit()
    else:
        print("Found {} matching .json files".format(len(json_files)))

    print("Creating and cleaning dataframes...")
    if args.n_procs > 1:
        p = Pool(args.n_procs)
        dfs = p.map(create_dataframe, json_files)
    else:
        dfs = [create_dataframe(file) for file in json_files]

    # Get rid of all empty entries
    dfs = [df for df in dfs if df is not None]

    if len(dfs) > 0:
        print("Combining dataframes...")
        result_df = pd.concat(dfs, axis=0)

        # Calculate the core hours for each simulation type
        if args.calc_core_hours:
            for sim_type in ProcTypeConst:
                if sim_type.value in result_df.columns.levels[0].values:
                    cur_df = result_df.loc[:, sim_type.value]

                    if (
                        MetaConst.run_time.value in cur_df.columns
                        and MetaConst.n_cores.value in cur_df.columns
                    ):
                        result_df.loc[
                            :, (sim_type.value, MetaConst.core_hours.value)
                        ] = (
                            cur_df.loc[:, MetaConst.run_time.value]
                            * cur_df.loc[:, MetaConst.n_cores.value]
                        )
                    # Missing run time and number of cores column
                    else:
                        print(
                            "Columns {} and {} do no exist for "
                            "simulation type {}".format(
                                MetaConst.run_time.value,
                                MetaConst.run_time.value,
                                sim_type.value,
                            )
                        )

        print("Saving the final dataframe in {}".format(args.output_file))
        result_df.to_csv(args.output_file)


if __name__ == "__main__":
    parser = ArgumentParser()

    parser.add_argument(
        "-i",
        "--input_dirs",
        type=str,
        nargs="+",
        help="Input directory/directories that contains the "
        "json files with the metadata",
    )
    parser.add_argument(
        "-o",
        "--output_file",
        type=str,
        help="The name of the file to save the " "resulting dataframe",
    )
    parser.add_argument(
        "-n", "--n_procs", type=int, default=4, help="Number of processes to use"
    )
    parser.add_argument(
        "-nr",
        "--not_recursive",
        action="store_true",
        help="Disables recursive file searching",
        default=False,
    )
    parser.add_argument(
        "-fp",
        "--filename_pattern",
        type=str,
        default="all_sims",
        help="The json file pattern to search. "
        "Do not add .json. Defaults to 'all_sims'.",
    )
    parser.add_argument(
        "--calc_core_hours",
        action="store_true",
        default=False,
        help="Calculates the total number of core hours "
        "from the run_time and number of cores and adds "
        "them to the dataframe as a column",
    )

    args = parser.parse_args()
    main(args)
