#!/usr/bin/env python
"""Calculates the median across the input files files for each IM/site pair
and saves the result in the specified csv file.
"""
import pandas as pd
import numpy as np

from argparse import ArgumentParser

from qcore.im import order_im_cols_df

STATION_COL_NAME = "station"
COMPONENT_COL_NAME = "component"
DEFAULT_COMPONENT = "geom"


def calc_im_median(input_files, output_file):
    """Calculates the IM median for each IM/site pair across the files

    Drops site entries for which component is not "geom"
    """
    # Load the csv files and drop non-geom rows
    dfs = []
    for cur_file in input_files:
        df = pd.read_csv(cur_file)

        mask = df[COMPONENT_COL_NAME].values == DEFAULT_COMPONENT
        dfs.append(df.loc[mask, :])

    # All IM columns and stations
    im_columns = np.unique(
        np.concatenate(
            [df.select_dtypes(include=np.float).columns.values for df in dfs]
        )
    )
    stations = np.unique(np.concatenate([df[STATION_COL_NAME].values for df in dfs]))

    # Pre-populate data with nan
    data = np.zeros(
        (stations.shape[0], im_columns.shape[0], len(input_files)), dtype=np.float32
    )
    data.fill(np.nan)

    # Create a 3d data array
    # with shape [n_stations, n_im_columns, n_input_files]
    for ix, df in enumerate(dfs):
        cur_columns_indices = np.flatnonzero(
            np.isin(im_columns, df.select_dtypes(include=np.float).columns.values)
        )
        cur_stations_indices = np.flatnonzero(
            np.isin(stations, df[STATION_COL_NAME].values)
        )

        cur_data = df[im_columns[cur_columns_indices]].values

        data[cur_stations_indices[:, None], cur_columns_indices, ix] = cur_data

    # Calculate the median along the input files axis, ignoring nan values
    median = np.nanmedian(data, axis=2)

    # Create the result dataframe
    result_df = pd.DataFrame(
        data=median, columns=["{}_median".format(col) for col in im_columns]
    )
    result_df[STATION_COL_NAME] = stations
    result_df[COMPONENT_COL_NAME] = DEFAULT_COMPONENT

    result_df = order_im_cols_df(result_df)

    result_df.to_csv(output_file, index=False, float_format="%.6f")


if __name__ == "__main__":
    parser = ArgumentParser(
        "Calculates the median across the input files for each IM/site "
        "pair and saves the result in the specified csv file."
    )

    parser.add_argument("output_file", help="Full path to the output csv file")
    parser.add_argument("input_files", help="Input IM csv files to average", nargs="+")

    args = parser.parse_args()

    calc_im_median(args.input_files, args.output_file)
