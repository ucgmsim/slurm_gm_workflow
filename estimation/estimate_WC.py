#!/usr/bin/env python3
"""
Functions for easy estimation of WC, uses pre-trained models.
"""
import os
import glob
import pickle

import numpy as np

from estimation.model import NNWcEstModel

LF_MODEL_DIR = "/home/cbs51/code/slurm_gm_workflow/estimation/models/LF"
HF_MODEL_DIR = "/home/cbs51/code/slurm_gm_workflow/estimation/models/HF"
BB_MODEL_DIR = "/home/cbs51/code/slurm_gm_workflow/estimation/models/BB"

MODEL_PREFIX = "model_"
SCALER_PREFIX = "scaler_"


def estimate_LF_WC_single(
        nx: float, ny: float, nz: float, nt: float, model_dir: str = LF_MODEL_DIR,
        model_prefix: str = MODEL_PREFIX, scaler_prefix: str = SCALER_PREFIX):
    """Convenience function to make a single estimation

    If the input parameters (or even just the order) of the model
    is ever changed, then this function has to be adjusted accordingly.

    Params
    ------
    nx, ny, nz, nt: float
        Input features for the model

    Returns
    -------
    wc: float
        Estimated wall clock time
    """
    # Make a numpy array of the input data in the right shape
    # The order of the features has to the same as for training!!
    data = np.array([nx, ny, nz, nt]).reshape(1, 4)

    return estimate(data, model_dir=model_dir, model_prefix=model_prefix,
                    scaler_prefix=scaler_prefix)


def estimate_HF_WC_single(
        fd_count: int, nsub_stoch: float, nt: float, model_dir: str = HF_MODEL_DIR,
        model_prefix: str = MODEL_PREFIX, scaler_prefix: str = SCALER_PREFIX):
    """Convenience function to make a single estimation

    If the input parameters (or even just the order) of the model
    is ever changed, then this function has to be adjusted accordingly.

    Params
    ------
    fd_count, nsub_stoch, nt: int, float
        Input features for the model

    Returns
    -------
    wc: float
        Estimated wall clock time
    """
    # Make a numpy array of the input data in the right shape
    # The order of the features has to the same as for training!!
    data = np.array([fd_count, nsub_stoch, nt]).reshape(1, 3)

    return estimate(data, model_dir=model_dir, model_prefix=model_prefix,
                    scaler_prefix=scaler_prefix)


def estimate_BB_WC_single(
        fd_count: int, nt: float, model_dir: str = BB_MODEL_DIR,
        model_prefix: str = MODEL_PREFIX, scaler_prefix: str = SCALER_PREFIX):
    """Convenience function to make a single estimation

    If the input parameters (or even just the order) of the model
    is ever changed, then this function has to be adjusted accordingly.

    Params
    ------
    fd_count, nt: int, float
        Input features for the model
        Where nt is the nt from HF


    Returns
    -------
    wc: float
        Estimated wall clock time
    """
    # Make a numpy array of the input data in the right shape
    # The order of the features has to the same as for training!!
    data = np.array([fd_count, nt]).reshape(1, 2)

    return estimate(data, model_dir=model_dir, model_prefix=model_prefix,
                    scaler_prefix=scaler_prefix)


def estimate(input_data: np.ndarray, model_dir: str,
             model_prefix: str = MODEL_PREFIX,
             scaler_prefix: str = SCALER_PREFIX):
    """Function to use for making estimations using a pre-trained model

    Scales the input data and then returns the estimations
    from the loaded model.

    Params
    ------
    input_data: np.ndarray
        Numpy array with shape [n_samples, n_features], where the features
        have to be the same (and in the same order) as when the model
        was trained)

    Returns
    -------
    wc: float
        Estimated wall clock time
    """
    model, scaler = load_model(model_dir, model_prefix, scaler_prefix)

    data = scaler.transform(input_data)
    wc = model.predict(data)

    return wc


def load_model(dir: str, model_prefix: str, scaler_prefix: str):
    """Loads a model and its associated standard scaler

    If there are several models in the specified directory, then the latest
    one is loaded (based on its timestamp)
    """
    file_pattern = os.path.join(dir, "{}{}".format(model_prefix, "*.h5"))
    model_files = glob.glob(file_pattern)

    model_file, scaler_file = None, None
    if len(model_files) == 0:
        raise Exception("No valid model was found with "
                        "file pattern {}".format(file_pattern))
    elif len(model_files) == 0:
        model_file = model_files[0]
    else:
        # Grab the newest model
        model_files.sort()
        model_file = model_files[-1]

    scaler_file = os.path.join(
        os.path.dirname(model_file),
        os.path.basename(model_file).replace(
            model_prefix, scaler_prefix).replace(".h5", ".pickle"))

    if not os.path.isfile(scaler_file):
        raise Exception(
            "No matching scaler was found for model {}".format(model_file))

    with open(scaler_file, 'rb') as f:
        scaler = pickle.load(f)

    model = NNWcEstModel.from_saved_model(model_file)

    print("Loaded model {} and scaler {}".format(model_file, scaler_file))

    return model, scaler






