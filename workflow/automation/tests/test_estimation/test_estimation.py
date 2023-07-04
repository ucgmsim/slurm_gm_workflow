"""Unitests for estimation, the aim is to
a) ensure that the estimate_wct workflow works 
"""
import numpy as np
import pytest
from qcore import config

import workflow.automation.estimation.estimate_wct

# Test data
# small = Hossack
# med = RitchieW2
# large = WairarapNich

# ( (nx, ny, nz, nt, fd_count, ncore), true_core_hours)
LF_SMALL = ((85.0, 88.0, 90.0, 940, 141, 160.0), 0.105)
LF_MED = ((198.0, 231.0, 102.0, 2240, 16, 160.0), 0.825)
LF_LARGE = ((735.0, 1073.0, 182.0, 9934, 5856, 160), 135.737)

# ( (fd_count, nsub_stoch, nt, ncore), true_core_hours)
HF_SMALL = ((141, 10, 1880, 80), 0.131)
HF_MED = ((16, 126, 4480, 80), 1.273)
HF_LARGE = ((5856, 1716, 19868, 80), 29.842)

# ( (fd_count, nt, ncore), true_core_hours)
BB_SMALL = ((141, 1880, 80), 1.131)
BB_MED = ((16, 4480, 80), 1.360)
BB_LARGE = ((5856, 19868, 80), 3.044)

# ( (fd_count, nt, comp_count, pSA_count, ncore), true_core_hours)
IM_SMALL = ((141, 1880, 3, 15.0, 40), 0.097)
IM_MED = ((16, 4480, 3, 15.0, 40), 0.041)
IM_LARGE = ((5856, 19868, 3, 15.0, 40), 5.419)

##


def check_chours(est, true, tol):
    tol_margin = true * tol
    assert (est < true + tol_margin) and (est > true - tol_margin)


@pytest.mark.parametrize(
    ["data", "true", "tolerance"], [(*LF_SMALL, 0.1), (*LF_MED, 0.1), (*LF_LARGE, 0.1)]
)
def test_EMOD3D_single(data, true, tolerance):
    nx, ny, nz, nt, fd_count, ncores = data
    chours, *_ = workflow.automation.estimation.estimate_wct.est_LF_chours_single(
        nx, ny, nz, nt, fd_count, ncores, False
    )

    check_chours(chours, true, tolerance)


@pytest.mark.parametrize(
    ["data", "true", "tolerance"], [(*HF_SMALL, 0.1), (*HF_MED, 0.1), (*HF_LARGE, 0.1)]
)
def test_HF_single(data, true, tolerance):
    fd_count, nsub_stoch, nt, n_logical_cores = data
    chours, *_ = workflow.automation.estimation.estimate_wct.est_HF_chours_single(
        fd_count, nsub_stoch, nt, n_logical_cores, False
    )

    check_chours(chours, true, tolerance)


@pytest.mark.parametrize(
    ["data", "true", "tolerance"], [(*BB_SMALL, 0.1), (*BB_MED, 0.1), (*BB_LARGE, 0.1)]
)
def test_BB_single(data, true, tolerance):
    fd_count, nt, n_logical_cores = data
    chours, *_ = workflow.automation.estimation.estimate_wct.est_BB_chours_single(
        fd_count, nt, n_logical_cores
    )

    check_chours(chours, true, tolerance)


@pytest.mark.parametrize(
    ["data", "true", "tolerance"], [(*IM_SMALL, 0.1), (*IM_MED, 0.1), (*IM_LARGE, 0.1)]
)
def test_IM_single(data, true, tolerance):
    fd_count, nt, im_comp_count, pSA_count, n_cores = data
    chours, *_ = workflow.automation.estimation.estimate_wct.est_IM_chours(
        fd_count, nt, im_comp_count, pSA_count, n_cores
    )

    check_chours(chours, true, tolerance)


# Tests based on Maui parameters
host, host_config_path = config.determine_machine_config("maui")
qconfig = config.get_machine_config(config_path=host_config_path)

MAX_JOB_WCT = qconfig[config.ConfigKeys.MAX_JOB_WCT.name]
MAX_NODES_PER_JOB = qconfig[config.ConfigKeys.MAX_NODES_PER_JOB.name]
PHYSICAL_NCORES_PER_NODE = qconfig[config.ConfigKeys.cores_per_node.name]
MAX_CH_PER_JOB = qconfig[config.ConfigKeys.MAX_CH_PER_JOB.name]


@pytest.mark.parametrize(
    ["in_params", "out_count", "out_time"],
    [
        (
            (PHYSICAL_NCORES_PER_NODE * 2, MAX_JOB_WCT / 2),
            PHYSICAL_NCORES_PER_NODE * 2,
            MAX_JOB_WCT / 2,
        ),
        (
            (PHYSICAL_NCORES_PER_NODE, MAX_JOB_WCT + 1),
            PHYSICAL_NCORES_PER_NODE * 2,
            (MAX_JOB_WCT + 1) / 2,
        ),
        (
            (PHYSICAL_NCORES_PER_NODE * MAX_NODES_PER_JOB * 2, MAX_JOB_WCT / 2),
            PHYSICAL_NCORES_PER_NODE * MAX_NODES_PER_JOB,
            MAX_CH_PER_JOB / PHYSICAL_NCORES_PER_NODE / MAX_NODES_PER_JOB,
        ),
        (
            (PHYSICAL_NCORES_PER_NODE * (MAX_NODES_PER_JOB + 1), MAX_JOB_WCT + 1),
            PHYSICAL_NCORES_PER_NODE * MAX_NODES_PER_JOB,
            MAX_CH_PER_JOB / PHYSICAL_NCORES_PER_NODE / MAX_NODES_PER_JOB,
        ),
        (
            (PHYSICAL_NCORES_PER_NODE * (MAX_NODES_PER_JOB - 1), MAX_JOB_WCT - 1),
            PHYSICAL_NCORES_PER_NODE * (MAX_NODES_PER_JOB - 1),
            MAX_CH_PER_JOB / PHYSICAL_NCORES_PER_NODE / (MAX_NODES_PER_JOB - 1),
        ),
    ],
)
def test_confine_wct_node_parameters(in_params, out_count, out_time):
    (
        test_count,
        test_time,
    ) = workflow.automation.estimation.estimate_wct.confine_wct_node_parameters(
        *in_params,
        max_wct=MAX_JOB_WCT,
        max_core_count=MAX_NODES_PER_JOB * PHYSICAL_NCORES_PER_NODE,
        max_core_hours=MAX_CH_PER_JOB,
        cores_per_node=PHYSICAL_NCORES_PER_NODE,
        hyperthreaded=False,
        can_checkpoint=True,
        ch_safety_factor=1.0,
    )

    assert np.isclose(test_count, out_count)
    assert np.isclose(test_time, out_time)
