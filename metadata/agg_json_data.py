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
from typing import List

from multiprocessing import Pool
from argparse import ArgumentParser

from estimation.estimate_wct import get_IM_comp_count
from qcore.constants import (
    ProcessType,
    MetadataField,
    Components,
    Status,
    METADATA_TIMESTAMP_FMT,
)

DATE_COLUMNS = ["end_time", "start_time", "submit_time"]


def load_metadata_df(csv_file: str):
    """Loads the metadata dataframe and converts the columns to the
    correct types"""
    df = pd.read_csv(csv_file, index_col=[0], header=[0, 1])

    for col in df.columns:
        if col[1] in DATE_COLUMNS:
            df[col] = pd.to_datetime(df[col])

    return df





def get_row(json_file):
    """Gets a row of metadata for the single simulation json log file"""
    with open(json_file) as f:
        data_dict = json.load(f)
    print("get_row data_dict", data_dict)

    sim_name = data_dict.get(MetadataField.sim_name.value)
    if sim_name is None:
        print("No simulation name found in json file {}, skipping.".format(json_file))
        return None, None, None

    columns = []
    data = []

    # Iterate over the json and aggregate the data
    for proc_type in data_dict.keys():
        #print("proc type in data_dict keys", proc_type)
        if ProcessType.has_str_value(proc_type):
            #print("ProcessType.has_str_value(proc_type)",proc_type)
            for metadata_field in data_dict[proc_type].keys():
               # print("for metadata_field in data_dict[proc_type].keys()", metadata_field,data_dict[proc_type].keys())
                if MetadataField.is_substring(metadata_field):
                    #print("MetadataField.has_value(metadata_field)",metadata_field)
                    # Special handling as dataframes do not like lists
                    if MetadataField.im_comp.value in metadata_field and MetadataField.im_comp_count.value not in metadata_field:   # excludes "im_components_count"
                        for comp in data_dict[proc_type][metadata_field]:
                          #  print("comps",data_dict[proc_type][metadata_field])
                            columns.append((proc_type, comp))
                            data.append(1)
                        continue

                    # Adjust hyperthreaded number of cores to physical cores
                    if (
                        MetadataField.n_cores.value in metadata_field
                        and ProcessType.from_str(proc_type).is_hyperth
                    ):
                        columns.append((proc_type, metadata_field))
                        data.append(data_dict[proc_type][metadata_field] / 2.0)
                        continue

                    columns.append((proc_type, metadata_field))
                    data.append(data_dict[proc_type][metadata_field])

    return sim_name, columns, data


def convert_df(df: pd.DataFrame):
    """Convert columns to numeric data type (float) or date if specified"""
    # Iterate BB, HF, LF
    for proc_type in ProcessType.iterate_str_values():
        if proc_type in df.columns.levels[0].values:
            # All available metadata
            for meta_col in df[proc_type].columns.values:
                print("mmmmmmmmmmmmmmmmmmm", meta_col)
                # Convert date strings to date type
                if "time" in meta_col and "run" not in meta_col:
                    print("time in meta cl")
                    df[proc_type, meta_col] = pd.to_datetime(
                        df[proc_type, meta_col],
                        format=METADATA_TIMESTAMP_FMT,
                        errors="coerce",
                    )
                # Convert components to boolean
                elif Components.has_value(meta_col):
                    df.loc[df[proc_type, meta_col].isna(), (proc_type, meta_col)] = 0.0
                    df[(proc_type, meta_col)] = df[(proc_type, meta_col)].astype(
                        np.bool
                    )
                # Try to convert everything else to numeric
                elif "status" in meta_col: # status, status_1
                    print("is ssssssssssssssssssssssstatus", df)
                    df[proc_type, meta_col] = df[proc_type, meta_col]
                    # )
                else:
                    df[proc_type, meta_col] = pd.to_numeric(
                        df[proc_type, meta_col], errors="coerce", downcast=None
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


def create_dataframe(json_files: List[str], n_procs: int, calc_core_hours: bool):
    """Aggregate the data from the different simualtion metadata json files
    and create a single dataframe with the data.

    Only data that is in the MetadataField constants enum will be saved, others are
    ignored.
    """
    print(
        "Getting metadata from each simulation using {} number of process".format(
            n_procs
        )
    )
    if n_procs > 1:
        p = Pool(n_procs)
        rows = p.map(get_row, json_files)
    else:
        rows = [get_row(file) for file in json_files]

    print("Creating dataframe...")
    df = None
    for sim_name, columns, data in rows:
        print(sim_name, columns, data)
        if sim_name is None and columns is None and data is None:
            continue

        # Create the dataframe
        if df is None:
            print("df is none")
            df = pd.DataFrame(
                index=[sim_name],
                columns=pd.MultiIndex.from_tuples(columns),
                data=np.asarray(data, dtype=object).reshape(1, -1),
            )
        else:
            # Check/Add missing columns
            print("else")
            column_mask = np.asarray(
                [True if col in df.columns else False for col in columns]
            )
            if np.any(~column_mask):
                for col in columns:
                    if col not in df.columns:
                        df[col] = np.nan

            # Add row data
            df.loc[sim_name, columns] = data

    # Clean the dataframe
    df = convert_df(df)

    # Calculate the core hours for each simulation type
    if calc_core_hours:
        for proc_type in ProcessType.iterate_str_values():
            if proc_type in df.columns.levels[0].values:
                get_core_hours(df, proc_type)

    # Add n_steps to EMOD3D
    if ProcessType.EMOD3D.str_value in df.columns:
        cur_df = df.loc[:, ProcessType.EMOD3D.str_value]
        df.loc[:, (ProcessType.EMOD3D.str_value, MetadataField.n_steps.value)] = (
            cur_df[MetadataField.nx.value]
            * cur_df[MetadataField.ny.value]
            * cur_df[MetadataField.nz.value]
        )
    return df


def get_core_hours(df, proc_type):
    cur_df = df.loc[:, proc_type]
    # Missing run time and number of cores column
    if (MetadataField.run_time.value not in cur_df.columns or MetadataField.n_cores.value not in cur_df.columns):
        print(
            "Columns {} and {} do no exist for "
            "simulation type {}".format(
                MetadataField.run_time.value,
                MetadataField.run_time.value,
                proc_type,
            )
        )
    else:
        for col in cur_df.columns:
            print("col", col)
            if MetadataField.run_time.value in col:  # run_time_1
                n_cores_col = col.replace(MetadataField.run_time.value, MetadataField.n_cores.value)  # cores_1
                if n_cores_col in cur_df.columns:
                    core_hours_col = col.replace(MetadataField.run_time.value, MetadataField.core_hours.value)  # core_hours_1
                    df.loc[:, (proc_type, core_hours_col)] = (
                        cur_df.loc[:, col]
                        * cur_df.loc[:, n_cores_col] / 3600.
                    )


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

    df = create_dataframe(json_files, args.n_procs, True)

    print("Saving the final dataframe in {}".format(args.output_file))
    df.to_csv(args.output_file)

    return df


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
        help="The name of the file to save the resulting dataframe",
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
        default="metadata_log",
        help="The json file pattern to search. "
        "Do not add .json. Defaults to 'metadata_log'.",
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
