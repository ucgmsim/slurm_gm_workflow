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
MAX_CH_PER_JOB = config.qconfig[config.ConfigKeys.MAX_CH_PER_JOB.name]

CH_SAFETY_FACTOR = 1.5
ERROR_MSG_SHAPE_MISMATCH = "Invalid input data, has to be {required_column_count} columns. One for each feature."


def confine_wct_node_parameters(
    core_count: int,
    run_time: float,
    max_wct: float = float(MAX_JOB_WCT),
    min_wct: float = (const.CHECKPOINT_DURATION * 3) / 60.0,
    max_core_count: int = MAX_NODES_PER_JOB * PHYSICAL_NCORES_PER_NODE,
    max_core_hours: int = MAX_CH_PER_JOB,
    cores_per_node: int = PHYSICAL_NCORES_PER_NODE,
    preserve_core_count: bool = False,
    full_node_only: bool = True,
    can_checkpoint: bool = False,
    hyperthreaded: bool = False,
    ch_safety_factor: float = CH_SAFETY_FACTOR,
    logger=get_basic_logger(),
):
    """
    Confines the parameters of a job to those of the queue it is to be submitted to.
    Assumes that a job requires a constant number of core hours regardless of core count
    (This is not true, as most jobs have a fixed start-up time, which will consume more CH with increasing core count).
    This function performs a three-step process:
    1) It ensures the total core hours is within the maximum allowed for a single job,
    2) If either of run time or core count are beyond the individual parameter limit it reduces that parameter to
    the limit.
    3) If the individual parameters are still beyond the maximum total core hours for the job then the wall
    clock time is reduced so that for the same job less total WCT will be required (ignoring time spent in the queue)
    :param run_time: The currently requested wall clock time to run for in hours
    :param core_count: The currently requested number of cores to use
    :param max_wct: The maximum wall clock time available for the current queue
    :param min_wct: The minimum wall clock time for a given job. Based on desired frequency of checkpointing, even if
    the job does not support checkpointing.
    :param max_core_count: The maximum core count possible for the current queue or job. Set to
    PHYSICAL_NCORES_PER_NODE to only use one node
    :param max_core_hours: The maximum core hours possible for a single job in the current queue
    :param cores_per_node: The number of cores on each node
    :param preserve_core_count: Maintain the number of cores to be used. Used for jobs that are being retried
    :param full_node_only: Set to False if the job can run on partial nodes,
    otherwise only whole nodes will be allocated
    :param hyperthreaded: If the core count given is hyperthreaded this must be taken into account
    :param can_checkpoint: If the job cannot checkpoint and the requested CH is greater than the available CH the job
    cannot run
    :param ch_safety_factor: A ch safety factor to add where possible, to try and ensure the job doesn't take just
    slightly longer than estimation calculates, significantly increasing the time required to complete
    :param logger: The logger to send messages to
    :return: A tuple containing the constrained core count and run time
    """
    if hyperthreaded:
        core_count /= 2

    ch = run_time * core_count * ch_safety_factor

    if full_node_only:
        scale_cc = lambda ch, max_wct: cores_per_node * np.ceil((ch / max_wct) / cores_per_node)
    else:
        scale_cc = lambda ch, max_wct: np.ceil(ch/max_wct)

    if ch > max_core_hours:
        if run_time * core_count > max_core_hours and not can_checkpoint:
            raise AssertionError(
                f"Job has greater core hours ({ch}) required than are available for a single job on this platform "
                f"({max_core_hours}). This job is unable to run successfully on this platform."
            )
        logger.debug(
            f"Job requires {ch} core hours, but the queue only allows {max_core_hours}, reducing. "
            f"This job will have to rely on checkpointing to complete across multiple submissions."
        )
        ch = max_core_hours

    if core_count > max_core_count:
        if preserve_core_count:
            raise AssertionError(
                f"Core count ({core_count}) greater than maximum core count ({max_core_count}), "
                f"but preserve_core_count is True."
            )
        logger.debug(
            f"Job had {core_count} cores which is greater than max allowed core count of {max_core_count}, "
            f"reducing core count and increasing wall clock time"
        )
        core_count = max_core_count
        run_time = min(ch / max_core_count, max_wct)
    elif run_time > max_wct:
        logger.debug(
            f"Job had {run_time} wall clock time which is greater than max allowed run time of {max_wct}, "
            f"reducing wall clock time"
            + (
                " and potentially increasing core count"
                if not preserve_core_count
                else ""
            )
        )
        if not preserve_core_count:
            core_count = min(
                scale_cc(ch, max_wct),
                max_core_count,
            )
        run_time = min(ch / core_count, max_wct)
    elif core_count * run_time > max_core_hours:
        logger.debug(
            f"Job parameters total ch ({core_count*run_time}) still beyond max core-hour bounds ({max_core_hours}). "
            f"Reducing wall clock time to fit."
        )
        run_time = max_core_hours / core_count
    elif core_count * run_time < ch:
        logger.debug(
            f"Job parameters total ch ({core_count * run_time}) below minimum core-hour bounds ({max_core_hours}). "
            f"Reducing wall clock time to fit."
        )
        run_time = min(ch / core_count, max_wct)
        if not preserve_core_count:
            core_count = min(
                scale_cc(ch, max_wct),
                max_core_count,
            )

    if run_time < min_wct:
        run_time = min_wct

    if hyperthreaded:
        core_count *= 2

    return int(core_count), run_time


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
    required_column_count = 6
    if data.shape[1] != required_column_count:
        raise Exception(
            ERROR_MSG_SHAPE_MISMATCH.format(required_column_count=required_column_count)
        )
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

    if config is not None and hasattr(config, "host") and config.host == "nurion":
        core_hours *= 4

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
    required_column_count = 4
    if data.shape[1] != required_column_count:
        raise Exception(
            ERROR_MSG_SHAPE_MISMATCH.format(required_column_count=required_column_count)
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

    if config is not None and hasattr(config, "host") and config.host == "nurion":
        core_hours *= 6

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
    required_column_count = 3
    if data.shape[1] != required_column_count:
        raise Exception(
            ERROR_MSG_SHAPE_MISMATCH.format(required_column_count=required_column_count)
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


def est_VM_PERT_chours_single(nx: int, ny: int, nz: int, n_cores: int):
    data = np.array([int(nx) * int(ny) * int(nz), int(n_cores)]).reshape(1, 2)

    core_hours, run_time = est_VM_PERT_chours(data)
    return core_hours[0], run_time[0]


def est_VM_PERT_chours(data: np.ndarray):
    required_column_count = 2
    if data.shape[1] != required_column_count:
        raise Exception(
            ERROR_MSG_SHAPE_MISMATCH.format(required_column_count=required_column_count)
        )

    coefficients = {
        "a": 5.917_060_637_004_801e-20,
        "b": 1.859_169_281_537_847e-09,
        "c": 0.130_000_000_000_000_4,
    }

    vm_size = data[:, 0]

    core_hours = (
        ((vm_size**2) * coefficients["a"])
        + (vm_size * coefficients["b"])
        + coefficients["c"]
    )

    return core_hours, core_hours / data[:, -1]


def est_IM_chours(
    fd_count: int,
    nt: Union[int, np.ndarray],
    comp: Union[List[str], int],
    pSA_count: int,
    n_cores: int,
    scale_ncores: bool = True,
    node_time_th_factor: int = 1,
):
    """Convenience function to make either a single or multiple estimations

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
    # TODO: adjust this number when its no longer hard-coded deep within IM_calc
    ROTD_THETA = 180
    est_rotd = False

    if isinstance(comp, list):
        comp_count = get_IM_comp_count(comp)
        est_rotd = any("rotd" in c for c in comp)
    else:
        comp_count = comp

    if est_rotd:
        coefficients = {"a": 0.855_946_934_326_698_3, "b": -19.063_651_268_616_194}
        core_hours = np.exp(
            (coefficients["a"] * np.log(nt * fd_count * pSA_count * ROTD_THETA))
            + coefficients["b"]
        )
    else:
        coefficients = {"a": 0.660_447_17, "b": -11.301_499_255_786_645}
        core_hours = np.exp(
            (coefficients["a"] * np.log(nt * fd_count * comp_count)) + coefficients["b"]
        )

    if config is not None and hasattr(config, "host") and config.host == "nurion":
        core_hours *= 7.5

    wct = core_hours / n_cores
    if scale_ncores and np.any(
        wct > (node_time_th_factor * n_cores / PHYSICAL_NCORES_PER_NODE)
    ):
        # Make a numpy array of the input data in the right shape
        data = np.array([int(n_cores)]).reshape(1, 1)
        core_hours, wct, n_cores = scale_core_hours(
            core_hours, data, node_time_th_factor
        )
        if not hasattr(nt, "__iter__"):
            core_hours, n_cores = float(core_hours), int(n_cores)

    return core_hours, core_hours / n_cores, n_cores


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
