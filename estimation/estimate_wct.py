#!/usr/bin/env python3
"""
Functions for easy estimation of WC, uses pre-trained models.
"""
import os
import glob
import pickle
from typing import List

import numpy as np

from estimation.model import NNWcEstModel

# Better solution for these locations?
LF_MODEL_DIR = "/nesi/project/nesi00213/estimation/models/LF/"
HF_MODEL_DIR = "/nesi/project/nesi00213/estimation/models/HF/"
BB_MODEL_DIR = "/nesi/project/nesi00213/estimation/models/BB/"
IM_MODEL_DIR = "/nesi/project/nesi00213/estimation/models/IM/"

MODEL_PREFIX = "model_"
SCALER_PREFIX = "scaler_"

# HF and BB use hyperthreading, hence they use a single node each on maui
# There is no number of nodes scaling.
HF_DEFAULT_NCORES = 80
BB_DEFAULT_NCORES = 80

# LF does not use hyperthreading, hence it uses 4 nodes by default with additional
# nodes added by scaling, explained in estimate_LF_chours
LF_DEFAULT_NCORES = 160
LF_DEFAULT_NCORES_PER_NODE = 40


def get_wct(run_time, overestimate_factor=0.1):
    """Pad the run time (in hours) by the specified factor.
    Then convert to wall clock time.

    Use this when estimation as max run time in a slurm script.
    """
    if run_time < (5.0 / 60.0):
        return convert_to_wct(5.0 / 60.0)
    else:
        return convert_to_wct(run_time * (1.0 + overestimate_factor))


def convert_to_wct(run_time):
    """Converts the run time (in hours) to a wall clock string"""
    return "{0:02.0f}:{1:02.0f}:00".format(*divmod(run_time * 60, 60))


def est_LF_chours_single(
    nx: int,
    ny: int,
    nz: int,
    nt: int,
    ncores: int,
    scale_ncores: bool,
    node_time_th_factor: float = 0.5,
    model_dir: str = LF_MODEL_DIR,
    model_prefix: str = MODEL_PREFIX,
    scaler_prefix: str = SCALER_PREFIX,
):
    """Convenience function to make a single estimation

    If the input parameters (or even just the order) of the model
    is ever changed, then this function has to be adjusted accordingly.

    Params
    ------
    nx, ny, nz, nt, n_cores: float, int
        Input features for the model
    scale_ncores: bool
        If True then the number of cores is adjusted until
        n_nodes * node_time_th == run_time
    node_time_th: float
        Node time threshold factor, does nothing if scale_ncores is not set

    Returns
    -------
    core_hours: float
        Estimated number of core hours
    run time: float
        Estimated run time (hours)
    n_cores: int
        The number of cores to use, returns the argument n_cores
        if scale_ncores is not set. Otherwise returns the updated ncores.
    """
    # Make a numpy array of the input data in the right shape.
    # The order of the features has to the same as for training!!
    data = np.array(
        [float(nx), float(ny), float(nz), float(nt), float(ncores)]
    ).reshape(1, 5)

    core_hours, run_time, ncores = estimate_LF_chours(
        data, scale_ncores, node_time_th_factor, model_dir, model_prefix, scaler_prefix
    )

    return core_hours[0], run_time[0], int(ncores[0])


def estimate_LF_chours(
    data: np.ndarray,
    scale_ncores: bool,
    node_time_th_factor: float = 0.5,
    model_dir: str = LF_MODEL_DIR,
    model_prefix: str = MODEL_PREFIX,
    scaler_prefix: str = SCALER_PREFIX,
):
    """
    Make bulk LF estimations, requires the input data to be in the right
    order!

    Params
    ------
    data: np.ndarray of float, int
        Input data for the model in the order nx, ny, nz, nt, n_cores
        Has to have a shape of [-1, 5]
    scale_ncores: bool
        If True then the number of cores is adjusted until
        n_nodes * node_time_th >= run_time
    node_time_th: float
        Node time threshold factor, does nothing if scale_ncores is not set

    Returns
    -------
    core_hours: np.ndarray of floats
        Estimated number of core hours
    run time: np.ndarray of floats
        Estimated run time (hours)
    n_cores: np.ndarray of ints
        The number of cores to use, returns the argument n_cores
        if scale_ncores is not set. Otherwise returns the updated ncores.
    """
    if data.shape[1] != 5:
        raise Exception(
            "Invalid input data, has to 5 columns. " "One for each feature."
        )

    core_hours = estimate(
        data,
        model_dir=model_dir,
        model_prefix=model_prefix,
        scaler_prefix=scaler_prefix,
    )

    # Just estimate, no number of cores scaling
    if not scale_ncores:
        return core_hours, core_hours / data[:, -1], data[:, -1]
    # Estimate and iteratively update the number of cores until
    # the threshold is meet
    else:
        run_time = core_hours / data[:, -1]
        time_th = node_time_th_factor * (data[:, -1] / LF_DEFAULT_NCORES_PER_NODE)
        mask = run_time > time_th
        while np.any(mask):
            data[mask, -1] += float(LF_DEFAULT_NCORES_PER_NODE)
            time_th[mask] = node_time_th_factor * (
                    data[mask, -1] / LF_DEFAULT_NCORES_PER_NODE
            )

            core_hours = estimate(
                data,
                model_dir=model_dir,
                model_prefix=model_prefix,
                scaler_prefix=scaler_prefix,
            )
            run_time = core_hours / data[:, -1]

        return core_hours, run_time, data[:, -1]


def est_HF_chours_single(
    fd_count: int,
    nsub_stoch: float,
    nt: int,
    n_cores: int,
    model_dir: str = HF_MODEL_DIR,
    model_prefix: str = MODEL_PREFIX,
    scaler_prefix: str = SCALER_PREFIX,
):
    """Convenience function to make a single estimation

    If the input parameters (or even just the order) of the model
    is ever changed, then this function has to be adjusted accordingly.

    Params
    ------
    fd_count, nsub_stoch, nt, n_cores: int, float
        Input features for the model

    Returns
    -------
    core_hours: float
        Estimated number of core hours
    run_time: float
        Estimated run time (hours)
    """
    # Make a numpy array of the input data in the right shape
    # The order of the features has to the same as for training!!
    data = np.array(
        [float(fd_count), float(nsub_stoch), float(nt), float(n_cores)]
    ).reshape(1, 4)

    core_hours, run_time = estimate_HF_chours(data, model_dir, model_prefix, scaler_prefix)

    return core_hours[0], run_time[0]


def estimate_HF_chours(
    data: np.ndarray,
    model_dir: str = HF_MODEL_DIR,
    model_prefix: str = MODEL_PREFIX,
    scaler_prefix: str = SCALER_PREFIX,
):
    """Make bulk HF estimations, requires data to be in the correct
    order (see above).

    Params
    ------
    data: np.ndarray of int, float
        Input data for the model in order fd_count, nsub_stoch, nt, n_cores
        Has to have shape [-1, 4]

    Returns
    -------
    core_hours: np.ndarray of floats
        Estimated number of core hours
    run_time: np.ndarray of floats
        Estimated run time (hours)
    """
    if data.shape[1] != 4:
        raise Exception(
            "Invalid input data, has to 4 columns. " "One for each feature."
        )

    core_hours = estimate(
        data,
        model_dir=model_dir,
        model_prefix=model_prefix,
        scaler_prefix=scaler_prefix,
    )
    return core_hours, core_hours / data[:, -1]


def est_BB_chours_single(
    fd_count: int,
    nt: int,
    n_cores: int,
    model_dir: str = BB_MODEL_DIR,
    model_prefix: str = MODEL_PREFIX,
    scaler_prefix: str = SCALER_PREFIX,
):
    """Convenience function to make a single estimation

    If the input parameters (or even just the order) of the model
    is ever changed, then this function has to be adjusted accordingly.

    Params
    ------
    fd_count, hf_nt, n_cores: int, float
        Input features for the model
        Where nt is the nt from HF

    Returns
    -------
    core_hours: float
        Estimated number of core hours
    run_time: float
        Estimated run time (hours)
    """
    # Make a numpy array of the input data in the right shape
    # The order of the features has to the same as for training!!
    data = np.array([float(fd_count), float(nt), float(n_cores)]).reshape(1, 3)

    core_hours, run_time = estimate_BB_chours(data, model_dir, model_prefix, scaler_prefix)

    return core_hours[0], run_time[0]


def estimate_BB_chours(
    data: np.ndarray,
    model_dir: str = BB_MODEL_DIR,
    model_prefix: str = MODEL_PREFIX,
    scaler_prefix: str = SCALER_PREFIX,
):
    """Make bulk BB estimations, requires data to be
    in the correct order (see above)

    Params
    ------
    data: np.ndarray of int, float
        Input data for the model in order fd_count, hf_nt, n_cores
        Has to have shape [-1, 3]

    Returns
    -------
    core_hours: np.ndarray of floats
        Estimated number of core hours
    run_time: np.ndarray of float
        Estimated run time (hours)
    """
    if data.shape[1] != 3:
        raise Exception(
            "Invalid input data, has to 3 columns. " "One for each feature."
        )

    core_hours = estimate(
        data,
        model_dir=model_dir,
        model_prefix=model_prefix,
        scaler_prefix=scaler_prefix,
    )

    return core_hours, core_hours / data[:, -1]


def est_IM_chours_single(
    fd_count: int,
    nt: int,
    comp: List[str],
    pSA_count: int,
    n_cores: int,
    model_dir: str = IM_MODEL_DIR,
    model_prefix: str = MODEL_PREFIX,
    scaler_prefix: str = SCALER_PREFIX,
):
    """Convenience function to make a single estimation

    If the input parameters (or even just the order) of the model
    is ever changed, then this function has to be adjusted accordingly.

    Params
    ------
    fd_count, nt, comp, pSA_count, n_cores: int, float
        Input features for the model. List of components is converted to
        number of components.

    Returns
    -------
    core_hours: float
        Estimated number of core hours
    run_time: float
        Estimated run time (hours)
    """
    # Make a numpy array of the input data in the right shape
    # The order of the features has to the same as for training!!
    data = np.array(
        [
            float(fd_count),
            float(nt),
            get_IM_comp_count(comp),
            float(pSA_count),
            float(n_cores),
        ]
    ).reshape(1, 5)

    core_hours = estimate(
        data,
        model_dir=model_dir,
        model_prefix=model_prefix,
        scaler_prefix=scaler_prefix,
    )[0]

    return core_hours, core_hours / n_cores


def get_IM_comp_count(comp: List[str]):
    """Counts the components, geom is considered 0.5 + 1 for each of
    the two components required to calculate it. I.e. if its only geom
    then 2.5.
    Ellipsis means everything, so just 3.5
    Other components are just counted as one.
    E.g. If geom is specfied and 000 (used for geom calculation)
    the result is still 2.5, as the other geom component (090) would
    still be calulcated.
    """
    if "ellipsis" in comp:
        return 3.5

    # Count geom as 2.5
    if len(comp) == 1 and comp[0] == "geom":
        return 2.5

    count = len(comp)
    if "geom" in comp:
        # Add count for geom components not specified explicitly
        count = (
            count
            - 0.5
            + len([1 for geom_comp in ["000", "090"] if geom_comp not in comp])
        )
    return count


def estimate(
    input_data: np.ndarray,
    model_dir: str,
    model_prefix: str = MODEL_PREFIX,
    scaler_prefix: str = SCALER_PREFIX,
):
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

    return wc.reshape(-1)


def load_model(dir: str, model_prefix: str, scaler_prefix: str):
    """Loads a model and its associated standard scaler

    If there are several models in the specified directory, then the latest
    one is loaded (based on its timestamp)
    """
    file_pattern = os.path.join(dir, "{}{}".format(model_prefix, "*.h5"))
    model_files = glob.glob(file_pattern)

    model_file, scaler_file = None, None
    if len(model_files) == 0:
        raise Exception(
            "No valid model was found with file pattern {}".format(file_pattern)
        )
    elif len(model_files) == 0:
        model_file = model_files[0]
    else:
        # Grab the newest model
        model_files.sort()
        model_file = model_files[-1]

    scaler_file = os.path.join(
        os.path.dirname(model_file),
        os.path.basename(model_file)
        .replace(model_prefix, scaler_prefix)
        .replace(".h5", ".pickle"),
    )

    if not os.path.isfile(scaler_file):
        raise Exception("No matching scaler was found for model {}".format(model_file))

    with open(scaler_file, "rb") as f:
        scaler = pickle.load(f)

    model = NNWcEstModel.from_saved_model(model_file)

    print("Loaded model {} and scaler {}".format(model_file, scaler_file))

    return model, scaler
