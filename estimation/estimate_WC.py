#!/usr/bin/env python3
"""
Functions for easy estimation of WC, uses pre-trained models.
"""
import os
import glob
import pickle

import numpy as np

from estimation.model import NNWcEstModel

# Better solution for these locations?
LF_MODEL_DIR = "/nesi/project/nesi00213/estimation/models/LF/"
HF_MODEL_DIR = "/nesi/project/nesi00213/estimation/models/HF/"
BB_MODEL_DIR = "/nesi/project/nesi00213/estimation/models/BB/"

MODEL_PREFIX = "model_"
SCALER_PREFIX = "scaler_"

HF_DEFAULT_NCORES = 80


def get_wct(run_time, overestimate_factor=0.1):
    """Pad the run time (in hours) by the specified factor.
    Then convert to wall clock time.

    Use this when estimation as max run time in a slurm script.
    """
    return convert_to_wct(run_time * (1.0 + overestimate_factor))


def convert_to_wct(run_time):
    """Converts the run time (in hours) to a wall clock string"""
    return '{0:02.0f}:{1:02.0f}:00'.format(*divmod(run_time * 60, 60))


def estimate_LF_WC_single(
        nx: int, ny: int, nz: int, nt: int, n_cores: int,
        model_dir: str = LF_MODEL_DIR, model_prefix: str = MODEL_PREFIX,
        scaler_prefix: str = SCALER_PREFIX):
    """Convenience function to make a single estimation

    If the input parameters (or even just the order) of the model
    is ever changed, then this function has to be adjusted accordingly.

    Params
    ------
    nx, ny, nz, nt, n_cores: float, int
        Input features for the model

    Returns
    -------
    wc: float
        Estimated wall clock time
    """
    # Make a numpy array of the input data in the right shape.
    # The order of the features has to the same as for training!!
    data = np.array([float(nx),
                     float(ny),
                     float(nz),
                     float(nt),
                     float(n_cores)]).reshape(1, 5)

    return estimate(data, model_dir=model_dir, model_prefix=model_prefix,
                    scaler_prefix=scaler_prefix)[0][0]


def estimate_HF_WC_single(
        fd_count: int, nsub_stoch: float, nt: int, n_cores: int,
        model_dir: str = HF_MODEL_DIR, model_prefix: str = MODEL_PREFIX,
        scaler_prefix: str = SCALER_PREFIX):
    """Convenience function to make a single estimation

    If the input parameters (or even just the order) of the model
    is ever changed, then this function has to be adjusted accordingly.

    Params
    ------
    fd_count, nsub_stoch, nt, n_cores: int, float
        Input features for the model

    Returns
    -------
    wc: float
        Estimated wall clock time
    """
    if n_cores != HF_DEFAULT_NCORES:
        print("WARNING: The model currently only supports estimation "
              "for {} number of cores. Therefore any estimation with a "
              "different number of cores will be very inaccurate.")

    # Make a numpy array of the input data in the right shape
    # The order of the features has to the same as for training!!
    data = np.array([float(fd_count),
                     float(nsub_stoch),
                     float(nt),
                     float(n_cores)]).reshape(1, 4)

    return estimate(data, model_dir=model_dir, model_prefix=model_prefix,
                    scaler_prefix=scaler_prefix)[0][0]


def estimate_BB_WC_single(
        fd_count: int, nt: int, model_dir: str = BB_MODEL_DIR,
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
    data = np.array([float(fd_count), float(nt)]).reshape(1, 2)

    return estimate(data, model_dir=model_dir, model_prefix=model_prefix,
                    scaler_prefix=scaler_prefix)[0][0]


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
    wc: np.ndarray
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






