#!/usr/bin/env python3
"""
Functions for easy estimation of WCT, uses formulas based on regression of prior core hour usage.

Note: The n_cores argument that most of these functions take, should be
the number of cores specified in the slurm script of the process type. So
these will be logical number of cores for some process types and physical for others.
"""
from typing import List, Union
from logging import Logger

import numpy as np

from qcore import config
import qcore.constants as const
from qcore.qclogging import get_basic_logger

MAX_JOB_WCT = config.qconfig[config.ConfigKeys.MAX_JOB_WCT.name]
MAX_NODES_PER_JOB = config.qconfig[config.ConfigKeys.MAX_NODES_PER_JOB.name]
PHYSICAL_NCORES_PER_NODE = config.qconfig[config.ConfigKeys.cores_per_node.name]

CH_SAFETY_FACTOR = 1.5


def get_wct(run_time, ch_safety_factor=CH_SAFETY_FACTOR):
    """Pad the run time (in hours) by the specified factor.
    Then convert to wall clock time.

    Use this when estimation as max run time in a slurm script.
    """
    wct_with_safety_factor = run_time * ch_safety_factor
    if wct_with_safety_factor < ((const.CHECKPOINT_DURATION * 3) / 60.0):
        return convert_to_wct((const.CHECKPOINT_DURATION * 3) / 60.0)
    else:
        return convert_to_wct(wct_with_safety_factor)


def convert_to_wct(run_time):
    """Converts the run time (in hours) to a wall clock string"""
    return "{0:02.0f}:{1:02.0f}:00".format(*divmod(run_time * 60, 60))


def est_LF_chours_single(
    nx: int,
    ny: int,
    nz: int,
    nt: int,
    fd_count: int,
    ncores: int,
    scale_ncores: bool,
    node_time_th_factor: float = 0.25,
):
    """Convenience function to make a single estimation

    If the input parameters (or even just the order) of the model
    is ever changed, then this function has to be adjusted accordingly.

    Params
    ------
    nx, ny, nz, nt, fd_count : float, int
        Input features for the model
    scale_ncores : bool
        If True then the number of cores is adjusted until
        n_nodes * node_time_th == run_time
    node_time_th : float
        Node time threshold factor, does nothing if scale_ncores is not set

    Returns
    -------
    core_hours : float
        Estimated number of core hours
    run time : float
        Estimated run time (hours)
    n_cores : int
        The number of cores to use, returns the argument n_cores
        if scale_ncores is not set. Otherwise returns the updated ncores.
    """
    # Make a numpy array of the input data in the right shape.
    data = np.array([nx, ny, nz, nt, fd_count, ncores]).reshape(1, 6)

    core_hours, run_time, ncores = estimate_LF_chours(
        data, scale_ncores, node_time_th_factor=node_time_th_factor
    )

    return core_hours[0], run_time[0], int(ncores[0])


def estimate_LF_chours(
    data: np.ndarray, scale_ncores: bool, node_time_th_factor: float = 0.25
):
    """
    Make bulk LF estimations, requires the input data to be in the right
    order!

    Params
    ------
    data: np.ndarray of float, int
        Input data in the order nx, ny, nz, nt, fd_count, n_cores
        Has to have a shape of [-1, 6]
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
    if data.shape[1] != 6:
        raise Exception("Invalid input data, has to 6 columns. One for each feature.")
    #
    coefficients = {
        "a": 0.804_038_96,
        "b": 0.090_770_8,
        "c": -18.992_825_379_162_817,
        "d": 0.282_455_29,
    }

    n_grid = data[:, 0] * data[:, 1] * data[:, 2]
    nt = data[:, 3]
    fd_count = data[:, 4]

    core_hours = np.exp(
        (coefficients["a"] * np.log(n_grid * nt))
        + coefficients["c"]
        + np.where(
            (np.log(fd_count)) > 6.5,
            (coefficients["b"] * np.log(fd_count))
            + (coefficients["d"] * (np.log(fd_count) - 6.5)),
            coefficients["b"] * np.log(fd_count),
        )
    )

    # data[:, -1] represents the last column of the ndarray data, which contains the number of cores for each task
    wct = core_hours / data[:, -1]

    if scale_ncores and np.any(
        wct > (node_time_th_factor * data[:, -1] / PHYSICAL_NCORES_PER_NODE)
    ):
        # Want to scale, and at least one job exceeds the allowable time for the given number of cores
        return scale_core_hours(core_hours, data, node_time_th_factor)
    else:
        return core_hours, core_hours / data[:, -1], (data[:, -1])


def est_HF_chours_single(
    fd_count: int,
    nsub_stoch: float,
    nt: int,
    n_logical_cores: int,
    scale_ncores: bool,
    node_time_th_factor: float = 1.0,
    logger: Logger = get_basic_logger(),
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
    data = np.array(
        [float(fd_count), float(nsub_stoch), float(nt), float(n_logical_cores)]
    ).reshape(1, 4)

    core_hours, run_time, n_cpus = estimate_HF_chours(
        data, scale_ncores, node_time_th_factor=node_time_th_factor, logger=logger
    )

    return core_hours[0], run_time[0], int(n_cpus[0])


def estimate_HF_chours(
    data: np.ndarray,
    scale_ncores: bool,
    node_time_th_factor: float = 1.0,
    logger: Logger = get_basic_logger(),
):
    """Make bulk HF estimations, requires data to be in the correct
    order (see above).

    Params
    ------
    data: np.ndarray of int, float
        Input data for the model in order fd_count, nsub_stoch, nt, n_cores
        Has to have shape [-1, 4]
    scale_ncores: bool
        If True then the number of cores is adjusted until
        n_nodes * node_time_th >= run_time
    node_time_th: float
        Node time threshold factor in hours, does nothing if scale_ncores is not set

    Returns
    -------
    core_hours: np.ndarray of floats
        Estimated number of core hours
    run_time: np.ndarray of floats
        Estimated run time (hours)
    n_cores: np.ndarray of ints
        The number of physical cores to use, returns the argument n_cores
        if scale_ncores is not set. Otherwise returns the updated ncores.
    """
    if data.shape[1] != 4:
        raise Exception(
            "Invalid input data, has to 4 columns. " "One for each feature."
        )

    hyperthreading_factor = 2.0 if const.ProcessType.HF.is_hyperth else 1.0

    # Adjust the number of cores to estimate physical core hours
    data[:, -1] = data[:, -1] / hyperthreading_factor

    coefficients = {
        "a": 7.430_968_49e-02,
        "b": 8.759_856_31e-01,
        "c": 1.274_082_95e-04,
        "d": -4.780_071_093_040_33,
    }

    fd_count = data[:, 0]
    nsub_stoch = data[:, 1]
    nt = data[:, 2]

    core_hours = np.exp(
        (coefficients["a"] * np.log(nt * np.log(nt)))
        + (coefficients["b"] * np.log(nsub_stoch))
        + (coefficients["c"] * fd_count)
        + coefficients["d"]
    )

    wct = core_hours / data[:, -1]
    if scale_ncores and np.any(
        wct > (node_time_th_factor * data[:, -1] / PHYSICAL_NCORES_PER_NODE)
    ):
        core_hours, wct, data[:, -1] = scale_core_hours(
            core_hours, data, node_time_th_factor
        )

    return core_hours, wct, data[:, -1] * hyperthreading_factor


def scale_core_hours(
    core_hours: np.ndarray, data: np.ndarray, node_time_th_factor: float
):
    """
    Estimate and update the number of nodes until
    the threshold is met. Assumes that core hours
    required remains constant with more nodes used.
    Find minimum number of cores such that:
    core_hours <= n_cores * node_time_th_factor * (n_cores/PHYSICAL_NCORES_PER_NODE)
    The right hand side of this formula determines
    the maximum time that a job may run for a given
    node_time_th_factor. n_cores is the number of cores,
    while n_cores/PHYSICAL_NCORES_PER_NODE is the number
    of nodes used, and multiplying this by the
    node_time_th_factor gives the maximum number of
    hours the job may run for.
    This allows the maximum wall clock time to scale
    with the number of nodes.
    If the estimated run time will be longer than
    a day, work out the minimum number of nodes
    required for it to run in less than 24 hours.
    Params
    ------
    core_hours: np.ndarray
        Initial estimated wall clock time
    data: np.ndarray of int, float
        Input data for the model in order fd_count, nsub_stoch, nt, n_cores
        Has to have shape [-1, 4]
    node_time_th_factor: float
        Node time threshold factor in hours, does nothing if scale_ncores is not set
    Returns
    -------
    core_hours: np.ndarray of floats
        Estimated number of core hours
    run_time: np.ndarray of floats
        Estimated run time (hours)
    n_cpus: np.ndarray of ints
        The number of cores to use, returns the argument n_cores
        if scale_ncores is not set. Otherwise returns the updated ncores.
    """

    # All computation is in terms of nodes
    n_nodes = data[:, -1] / PHYSICAL_NCORES_PER_NODE
    estimated_nodes = np.ceil(
        np.sqrt(core_hours / (node_time_th_factor * PHYSICAL_NCORES_PER_NODE))
    )
    mask = (estimated_nodes * node_time_th_factor) > MAX_JOB_WCT
    if np.any(mask):
        estimated_nodes[mask] = np.ceil(
            (core_hours[mask] / MAX_JOB_WCT) / PHYSICAL_NCORES_PER_NODE
        )
    n_nodes = np.minimum(np.maximum(estimated_nodes, n_nodes), MAX_NODES_PER_JOB)
    n_cpus = n_nodes * PHYSICAL_NCORES_PER_NODE
    return core_hours, core_hours / n_cpus, n_cpus


def est_BB_chours_single(fd_count: int, nt: int, n_logical_cores: int):
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
    data = np.array([float(fd_count), float(nt), float(n_logical_cores)]).reshape(1, 3)

    core_hours, run_time = estimate_BB_chours(data)

    return core_hours[0], run_time[0]


def estimate_BB_chours(
    data: np.ndarray,
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

    # Adjust the number of cores to estimate physical core hours
    data[:, -1] = data[:, -1] / 2.0 if const.ProcessType.BB.is_hyperth else data[:, -1]

    coefficients = {
        "a": 1.992_685_67e-01,
        "b": 8.158_222_65e-05,
        "c": -1.793_602_084_907_300_4,
    }

    fd_count = data[:, 0]
    nt = data[:, 1]

    core_hours = np.exp(
        (coefficients["a"] * np.log(nt * np.log(nt)))
        + (coefficients["b"] * fd_count)
        + coefficients["c"]
    )

    return core_hours, core_hours / data[:, -1]


def est_IM_chours_single(
    fd_count: int, nt: int, comp: Union[List[str], int], pSA_count: int, n_cores: int
):
    """Convenience function to make a single estimation

    If the input parameters (or even just the order) of the model
    is ever changed, then this function has to be adjusted accordingly.

    Params
    ------
    fd_count, nt, comp, pSA_count, n_cores: int, float
        Input features for the model. List of components is converted to
        number of components.
    n_cores: int
        IM_calc does not use hyperthreading, therefore these are the physical
        number of cores used.

    Returns
    -------
    core_hours: float
        Estimated number of core hours
    run_time: float
        Estimated run time (hours)
    """
    if isinstance(comp, list):
        comp_count = get_IM_comp_count(comp)
    else:
        comp_count = comp
    # Make a numpy array of the input data in the right shape
    data = np.array(
        [float(fd_count), float(nt), comp_count, float(pSA_count), float(n_cores)]
    ).reshape(1, 5)

    coefficients = {"a": 0.660_447_17, "b": -11.301_499_255_786_645}

    fd_count = data[:, 0]
    nt = data[:, 1]

    core_hours = np.exp(
        (coefficients["a"] * np.log(nt * fd_count * comp_count)) + coefficients["b"]
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
