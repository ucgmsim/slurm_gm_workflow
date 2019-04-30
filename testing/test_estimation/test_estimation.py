"""Unitests for estimation, the aim is to
a) ensure that the estimate_wct workflow works and
b) prevent models with large errors from being used
"""
import pytest

import estimation.estimate_wct as est
import qcore.constants as const


# Test data
# Hossack_HYP03-10_S1264, v18p6_rerun
LF_SMALL = ((139.0, 137.0, 83.0, 1821, 160.0), 0.1776)

# RitchieW2_HYP12-25_S1354, v18p6_rerun
LF_MED = ((380.0, 661.0, 110.0, 5436.0, 160.0), 4.7552)

# WairarapNich_HYP08-47_S1314, v18p6_rerun
LF_LARGE = ((1124.0, 2056.0, 158.0, 16798.0, 160), 159.6448)

# EdgecumbCS_HYP06-11_S1294, v18p6_rerun
HF_SMALL = ((1021.0, 25.0, 12924.0, 80), 0.0668)

# RuahineRev_HYP25-25_S1484, v18p6_rerun
HF_MED = ((4659.0, 126.0, 20884.0, 80), 2.9)

# WairarapNich_HYP18-47_S1414, v18p6_rerun
HF_LARGE = ((22869.0, 1694.0, 67192.0, 80), 358.5668)

# Chalky1to3_HYP06-24_S1294, v18p6_rerun
BB_SMALL = ((299.0, 12280, 80), 0.1)

# HopeCW_HYP13-25_S1364
BB_MED = ((4164.0, 20636.0, 80), 0.5)

# WairarapNich_HYP24-47_S1474
BB_LARGE = ((22869.0, 67192.0, 80), 16.7888)

# PangopangoF29_HYP10-10_S1334
IM_SMALL = ((199.0, 8112.0, 2.5, 15.0, 40), 0.0444)

# HopeCW_HYP12-25_S1354
IM_MED = ((4164.0, 20636.0, 2.5, 15.0, 40), 1.2776)

# AlpineF2K_HYP03-47_S1264
IM_LARGE = ((13621.0, 71820.0, 2.5, 15.0, 40), 11.4888)


# Fixtures for loading the different models
@pytest.fixture(scope="module")
def lf_NN_model():
    return est.load_full_model("../../estimation/models/LF/", const.EstModelType.NN)


@pytest.fixture(scope="module")
def lf_SVR_model():
    return est.load_full_model("../../estimation/models/LF/", const.EstModelType.SVR)


@pytest.fixture(scope="module")
def lf_combined_model():
    return est.load_full_model("../../estimation/models/LF/", const.EstModelType.NN_SVR)


@pytest.fixture(scope="module")
def hf_NN_model():
    return est.load_full_model("../../estimation/models/HF/", const.EstModelType.NN)


@pytest.fixture(scope="module")
def hf_SVR_model():
    return est.load_full_model("../../estimation/models/HF/", const.EstModelType.SVR)


@pytest.fixture(scope="module")
def bb_NN_model():
    return est.load_full_model("../../estimation/models/BB/", const.EstModelType.NN)


@pytest.fixture(scope="module")
def bb_SVR_model():
    return est.load_full_model("../../estimation/models/BB/", const.EstModelType.SVR)


@pytest.fixture(scope="module")
def im_NN_model():
    return est.load_full_model("../../estimation/models/IM/", const.EstModelType.NN)


@pytest.fixture(scope="module")
def im_SVR_model():
    return est.load_full_model("../../estimation/models/IM/", const.EstModelType.SVR)


def check_chours(est, true, tol):
    tol_margin = true * tol
    assert (est < true + tol_margin) and (est > true - tol_margin)


@pytest.mark.parametrize(
    ["data", "true", "tolerance"], [(*LF_SMALL, 1.5), (*LF_MED, 0.5), (*LF_LARGE, 0.1)]
)
def test_EMOD3D_NN_single(data, true, tolerance, lf_NN_model):
    nx, ny, nz, nt, ncores = data
    chours, *_ = est.est_LF_chours_single(
        nx, ny, nz, nt, ncores, lf_NN_model, False, model_type=const.EstModelType.NN
    )

    check_chours(chours, true, tolerance)


@pytest.mark.parametrize(
    ["data", "true", "tolerance"], [(*LF_MED, 1.0), (*LF_LARGE, 0.25)]
)
def test_EMOD3D_SVR_single(data, true, tolerance, lf_SVR_model):
    nx, ny, nz, nt, ncores = data
    chours, *_ = est.est_LF_chours_single(
        nx, ny, nz, nt, ncores, lf_SVR_model, False, model_type=const.EstModelType.SVR
    )

    check_chours(chours, true, tolerance)


@pytest.mark.parametrize(["data", "true", "tolerance"], [(*LF_MED, 0.5)])
def test_EMOD3D_comb_single(data, true, tolerance, lf_combined_model):
    """Just testing the workflow, as this will just use the NN model"""
    nx, ny, nz, nt, ncores = data
    chours, *_ = est.est_LF_chours_single(
        nx,
        ny,
        nz,
        nt,
        ncores,
        lf_combined_model,
        False,
        model_type=const.EstModelType.NN_SVR,
    )

    check_chours(chours, true, tolerance)


@pytest.mark.parametrize(
    ["data", "true", "tolerance"], [(*HF_SMALL, 2.5), (*HF_MED, 0.25), (*HF_LARGE, 0.1)]
)
def test_HF_NN_single(data, true, tolerance, hf_NN_model):
    """Just testing the workflow, as this will just use the NN model"""
    fd_count, nsub_stoch, nt, n_logical_cores = data
    chours, *_ = est.est_HF_chours_single(
        fd_count,
        nsub_stoch,
        nt,
        n_logical_cores,
        hf_NN_model,
        False,
        model_type=const.EstModelType.NN,
    )

    check_chours(chours, true, tolerance)


@pytest.mark.parametrize(
    ["data", "true", "tolerance"], [(*HF_MED, 0.5), (*HF_LARGE, 0.1)]
)
def test_HF_SVR_single(data, true, tolerance, hf_SVR_model):
    """Just testing the workflow, as this will just use the NN model"""
    fd_count, nsub_stoch, nt, n_logical_cores = data
    chours, *_ = est.est_HF_chours_single(
        fd_count,
        nsub_stoch,
        nt,
        n_logical_cores,
        hf_SVR_model,
        False,
        model_type=const.EstModelType.SVR,
    )

    check_chours(chours, true, tolerance)


@pytest.mark.parametrize(
    ["data", "true", "tolerance"], [(*BB_SMALL, 2.5), (*BB_MED, 0.25), (*BB_LARGE, 0.2)]
)
def test_BB_NN_single(data, true, tolerance, bb_NN_model):
    """Just testing the workflow, as this will just use the NN model"""
    fd_count, nt, n_logical_cores = data
    chours, *_ = est.est_BB_chours_single(
        fd_count, nt, n_logical_cores, bb_NN_model, model_type=const.EstModelType.NN
    )

    check_chours(chours, true, tolerance)


@pytest.mark.parametrize(
    ["data", "true", "tolerance"], [(*BB_MED, 0.25), (*BB_LARGE, 0.2)]
)
def test_BB_NN_single(data, true, tolerance, bb_SVR_model):
    """Just testing the workflow, as this will just use the NN model"""
    fd_count, nt, n_logical_cores = data
    chours, *_ = est.est_BB_chours_single(
        fd_count, nt, n_logical_cores, bb_SVR_model, model_type=const.EstModelType.SVR
    )

    check_chours(chours, true, tolerance)


@pytest.mark.parametrize(
    ["data", "true", "tolerance"], [(*IM_SMALL, 2.0), (*IM_MED, 0.25), (*IM_LARGE, 0.1)]
)
def test_IM_NN_single(data, true, tolerance, im_NN_model):
    """Just testing the workflow, as this will just use the NN model"""
    fd_count, nt, im_comp_count, pSA_count, n_cores = data
    chours, *_ = est.est_IM_chours_single(
        fd_count,
        nt,
        im_comp_count,
        pSA_count,
        n_cores,
        im_NN_model,
        model_type=const.EstModelType.NN,
    )

    check_chours(chours, true, tolerance)

@pytest.mark.parametrize(
    ["data", "true", "tolerance"], [(*IM_MED, 0.25), (*IM_LARGE, 0.2)]
)
def test_IM_SVR_single(data, true, tolerance, im_SVR_model):
    """Just testing the workflow, as this will just use the NN model"""
    fd_count, nt, im_comp_count, pSA_count, n_cores = data
    chours, *_ = est.est_IM_chours_single(
        fd_count,
        nt,
        im_comp_count,
        pSA_count,
        n_cores,
        im_SVR_model,
        model_type=const.EstModelType.SVR,
    )

    check_chours(chours, true, tolerance)