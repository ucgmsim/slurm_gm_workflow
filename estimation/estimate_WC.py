"""
Functions for easy estimation of WC, uses pre-trained models.

Needs to stay python2 and 3 compatible.

It is worth noting that some of these functions (load_model) would
normally be added the model class. However since that is written in python3,
these have been added here.
"""
import os
import glob
import pickle
import keras

import numpy as np

LF_MODEL_DIR = "/home/cbs51/code/slurm_gm_workflow/estimation/models/LF"

MODEL_PREFIX = "model_"
SCALER_PREFIX = "scaler_"


def estimate_LF_WC_single(nx, ny, nz, nt, model_dir=LF_MODEL_DIR,
                   model_prefix=MODEL_PREFIX, scaler_prefix=SCALER_PREFIX):
    """Convenience function to make a single estimation

    If the input parameters (or even just the order) of the model
    is ever changed, then this function has to be adjusted accordingly.

    Params
    ------
    nx, ny, nz, ny: float
        Input features for the model

    Returns
    -------
    wc: float
        Estimated wall clock time
    """
    # Make a numpy array of the input data in the right shape
    # The order of the features has to the same as for training!!
    data = np.array([nx, ny, nz, nt]).reshape(1, 4)

    return estimate_LF_WC(data, model_dir=model_dir, model_prefix=model_prefix,
                          scaler_prefix=scaler_prefix)


def estimate_LF_WC(input_data, model_dir=LF_MODEL_DIR,
                   model_prefix=MODEL_PREFIX, scaler_prefix=SCALER_PREFIX):
    """Function to use for making estimations

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


def load_model(dir, model_prefix, scaler_prefix):
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

    with open(scaler_file, 'r') as f:
        scaler = pickle.load(f)

    model = keras.models.load_model(model_file)

    print("Loaded model {} and scaler {}".format(model_file, scaler_file))

    return model, scaler






