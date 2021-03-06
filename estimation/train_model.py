#!/usr/bin/env python3
"""Script to train a WC estimation model for either LF, HF or BB

Note: Does not require a HPC, will happily run on standard hardware.
"""
import json
import os
import glob
import datetime
import shutil
import pickle

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from argparse import ArgumentParser

from estimation.model import NNWcEstModel, SVRModel
from qcore.constants import MetadataField, EST_MODEL_NN_PREFIX, EST_MODEL_SVR_PREFIX

CONFIG_INPUT_COLS_KEY = "input_cols"
CONFIG_TARGET_COL_KEY = "target_col"


SCALER_FILENAME = "scaler_{}_{}.pickle"
NN_MODEL_FILENAME = "{}{}.h5".format(EST_MODEL_NN_PREFIX, "{}")
SVR_MODEL_FILENAME = "{}{}.pickle".format(EST_MODEL_SVR_PREFIX, "{}")

TIMESTAMP_TEMPLATE = "{0:%Y%m%d_%H%M%S}"

MODEL_TYPES = ["NN", "SVR"]


def show_loss(model: NNWcEstModel):
    """Displays a plot that shows loss and validation loss"""
    if model.is_trained:
        loss = model.hist.history["loss"]
        val_loss = model.hist.history.get("val_loss")

        fig, ax = plt.subplots()
        fig.tight_layout()

        ax.plot(np.arange(len(loss)), loss, label="loss", marker=".", markersize=5)

        if val_loss is not None:
            ax.plot(
                np.arange(len(val_loss)),
                val_loss,
                label="val_loss",
                marker=".",
                markersize=5,
            )
        ax.legend()

    plt.show()


def gen_fake_ncores_data(X: np.ndarray, y: np.ndarray, core_col_ix: int, n_cores: int):
    """Computes the run_time for the given samples for the specified number
    of cores. Assumes run time is inversely proportional to number of cores."""
    X_new = np.copy(X)
    X_new[:, core_col_ix] = n_cores

    y_new = (y * X[:, core_col_ix]) / X_new[:, core_col_ix]

    return X_new, y_new


def main(args):
    # Get the data files (dataframes/csv)
    files = args.input_files
    if args.input_dir is not None:
        files = glob.glob(os.path.join(args.input_dir, "*.csv"))

    # No input data
    if files is None:
        print(
            "No input_files or input_dir provided. One of input_files or "
            "input_dir has to be specified."
        )
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
        [
            data_df.loc[:, tuple(col_path)].values.reshape(-1, 1)
            for col_path in input_data_cols
        ],
        axis=1,
    )
    y = data_df.loc[:, tuple(target_col)].values

    # Determine if a NN or SVR model is trained
    if args.model_type is not None:
        if args.model_type not in MODEL_TYPES:
            print(
                "Invalid model type specified, has to be one of {}".format(MODEL_TYPES)
            )
        is_NN = True if args.model_type == "NN" else False
        is_SVR = True if args.model_type == "SVR" else False
    else:
        is_NN = True if "NN" in os.path.basename(args.config).upper() else False
        is_SVR = True if "SVR" in os.path.basename(args.config).upper() else False

    if is_NN is False and is_SVR is False:
        raise Exception(
            "No valid model specified. Either the model type has to be specified "
            "directly or the config file name has to contain the model type."
        )

    # Fake number of cores data generation
    if is_NN and args.fake_n_cores is not None:
        # Check that number of cores is actually an input feature
        col_names = [col_path[1] for col_path in input_data_cols]
        if MetadataField.n_cores.value not in col_names:
            print(
                "Number of cores is not one of the input features, ignoring "
                "fake_n_cores argument!"
            )
        else:
            print(
                "Creating fake n_cores data, "
                "for {} number of cores".format(args.fake_n_cores)
            )
            new_X_data, new_y_data = [], []
            for cur_n_cores in args.fake_n_cores:
                # Generate the fake data
                X_new, y_new = gen_fake_ncores_data(
                    X, y, col_names.index(MetadataField.n_cores.value), cur_n_cores
                )
                new_X_data.append(X_new)
                new_y_data.append(y_new)

            X = np.concatenate((X, np.concatenate(new_X_data, axis=0)), axis=0)
            y = np.concatenate((y, np.concatenate(new_y_data)))

    timestamp = TIMESTAMP_TEMPLATE.format(datetime.datetime.now())

    model = NNWcEstModel if is_NN else SVRModel

    X, y, _ = model.preprocessing(
        X,
        y,
        scaler_save_file=os.path.join(
            args.output_dir, SCALER_FILENAME.format("NN" if is_NN else "SVR", timestamp)
        ),
    )

    # Train and save the model
    if is_NN:
        model = NNWcEstModel(args.config)
        model.train(X, y, val_split=args.val_split, debug_dir=args.debug_dir)
        model.save_model(
            os.path.join(args.output_dir, NN_MODEL_FILENAME.format(timestamp))
        )

        if args.verbose:
            for key in model.hist.history.keys():
                print(
                    "Final epoch value for {}: {:.5f}".format(
                        key, model.hist.history[key][-1]
                    )
                )

        if args.show_loss:
            show_loss(model)
    elif is_SVR:
        model = SVRModel(args.config)
        model.train(X, y, val_split=args.val_split)
        model.save_model(
            os.path.join(args.output_dir, SVR_MODEL_FILENAME.format(timestamp))
        )

    # Also save the config file used
    shutil.copy(
        args.config,
        os.path.join(
            args.output_dir,
            "{}_{}.json".format(os.path.basename(args.config).split(".")[0], timestamp),
        ),
    )


if __name__ == "__main__":
    parser = ArgumentParser(
        description="Script to train a WC estimation model for "
        "either LF, HF or BB. The input data files have to have "
        "been created by agg_json_data or be in the exact "
        "same format"
    )

    parser.add_argument(
        "-od",
        "--output_dir",
        type=str,
        required=True,
        help="The directory to save the trained model in.",
    )
    parser.add_argument(
        "--config", type=str, help="The config file to use.", required=True
    )
    parser.add_argument(
        "--model_type",
        type=str,
        choices=MODEL_TYPES,
        help="What type of model to train. If this is not specified"
        "then this is determined based on the config file name.",
    )
    parser.add_argument(
        "-i",
        "--input_files",
        type=str,
        default=None,
        help="Input files to use for training. Overwritten"
        "if input_dir is set. Either input_files or"
        "input_dir has to be set.",
        nargs="+",
    )
    parser.add_argument(
        "-id",
        "--input_dir",
        type=str,
        default=None,
        help="Input dir which contains the files for training."
        "Overwrites input_files. Either input_files or"
        "input_dir has to be set.",
    )
    parser.add_argument(
        "--scaler_file",
        type=str,
        default=None,
        help="Path to the scaler file to use. By default a new scaler is created.",
    )
    parser.add_argument(
        "--val_split",
        type=float,
        default=None,
        help="Proportion of data to use for validation." "Default is 0.2.",
    )
    parser.add_argument(
        "--show_loss",
        action="store_true",
        default=False,
        help="If a NN model is used, show a " "loss and validation loss graph",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        help="Adds more verbosity",
    )
    parser.add_argument(
        "--fake_n_cores",
        type=int,
        default=None,
        help="Only valid for NN model. Use the training data "
        "with fake n_cores assumes run_time is proptional "
        "to the inverse of n_cores",
        nargs="+",
    )
    parser.add_argument(
        "--debug_dir",
        type=str,
        default=None,
        help="Only for NN training. Save tensorboard log files.",
    )

    args = parser.parse_args()

    main(args)
