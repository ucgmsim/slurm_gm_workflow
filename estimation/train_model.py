#!/usr/bin/env python3
"""Script to train a WC estimation model for either LF, HF or BB

Note: Does not require a HPC, will happily run on standard hardware.
"""
import json
import os
import glob
import datetime

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from argparse import ArgumentParser
from sklearn.preprocessing import StandardScaler
from sklearn.externals import joblib

from estimation.model import NNWcEstModel

CONFIG_INPUT_COLS_KEY = "input_cols"
CONFIG_TARGET_COL_KEY = "target_col"

SCALER_FILENAME = "scaler_{}.pickle"
MODEL_FILENAME = "model_{}.h5"


def preprocessing(X: np.ndarray, y: np.ndarray, scaler_file: str = None):
    """Performs the preprocessing

    Standardizes the input data (i.e. mean=0, std=1) for each feature.
    Removes any rows which contain a np.nan entry
    Saves scaler if file is specified

    Returns
    -------
    Scaled X, y, StandardScaler instance used
    """
    # Remove nan entries
    row_nan_mask = np.any(np.isnan(X), axis=1) | np.isnan(y)
    X, y = X[~row_nan_mask, :], y[~row_nan_mask]

    # Scale
    std_scaler = StandardScaler()
    X = std_scaler.fit_transform(X)

    if scaler_file is not None:
        joblib.dump(std_scaler, scaler_file)

    return X, y, std_scaler


def show_loss(model: NNWcEstModel):
    """Displays a plot that shows loss and validation loss"""
    if model.is_trained:
        loss = model.hist.history['loss']
        val_loss = model.hist.history.get('val_loss')

        fig, ax = plt.subplots()
        fig.tight_layout()

        ax.plot(np.arange(len(loss)), loss, label="loss", marker='.',
                markersize=5)

        if val_loss is not None:
            ax.plot(np.arange(len(val_loss)), val_loss, label="val_loss",
                    marker='.', markersize=5)
        ax.legend()

    plt.show()


def main(args):
    # Get the data files (dataframes/csv)
    files = args.input_files
    if args.input_dir is not None:
        files = glob.glob(os.path.join(args.input_dir, "*.csv"))

    # No input data
    if files is None:
        print("No input_files or input_dir provided. One of input_files or "
              "input_dir has to be specified.")
        return

    # Load & combine the dataframes
    dfs = [pd.read_csv(file, index_col=[0], header=[0, 1]) for file in files]
    data_df = pd.concat(dfs, axis=0)

    # Get the relevant data
    with open(args.config, "r") as f:
        config_dict = json.load(f)

    input_data_cols = config_dict[CONFIG_INPUT_COLS_KEY]
    target_col = config_dict[CONFIG_TARGET_COL_KEY]

    X = np.concatenate(
        [data_df.loc[:, tuple(col_path)].values.reshape(-1, 1) for col_path in
         input_data_cols], axis=1)
    y = data_df.loc[:, tuple(target_col)].values

    timestamp = "{0:%Y%m%d_%H%M%S}".format(datetime.datetime.now())

    # Preprocessing
    X, y, _ = preprocessing(X, y, os.path.join(
        args.output_dir, SCALER_FILENAME.format(timestamp)))

    # Train and save the model
    model = NNWcEstModel(args.config)
    model.train(X, y, args.val_split)
    model.save_model(os.path.join(
        args.output_dir, MODEL_FILENAME.format(timestamp)))

    if args.show_loss:
        show_loss(model)


if __name__ == '__main__':
    parser = ArgumentParser(
        description="Script to train a WC estimation model for "
                    "either LF, HF or BB. The input data files have to have "
                    "been created by agg_json_data or be in the exact "
                    "same format")

    parser.add_argument("-od", "--output_dir", type=str, required=True,
                        help="The directory to save the trained model in.")
    parser.add_argument("--config", type=str,
                        help="The config file to use.", required=True)
    parser.add_argument("-i", "--input_files", type=str, default=None,
                        help="Input files to use for training. Overwritten"
                             "if input_dir is set. Either input_files or"
                             "input_dir has to be set.",
                        nargs="+")
    parser.add_argument("-id", "--input_dir", type=str, default=None,
                        help="Input dir which contains the files for training."
                             "Overwrites input_files. Either input_files or"
                             "input_dir has to be set.")
    parser.add_argument("--val_split", type=float, default=0.2,
                        help="Proportion of data to use for validation."
                             "Default is 0.2.")
    parser.add_argument("--show_loss", action="store_true", default=False,
                        help="If a NN model is used, show a "
                             "loss and validation loss graph")

    args = parser.parse_args()

    main(args)
