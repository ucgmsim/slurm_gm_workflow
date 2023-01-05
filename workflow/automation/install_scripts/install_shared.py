"""Module that contains shared install functions"""
import os
from logging import Logger

from numpy import isclose
import yaml

from qcore import geo, utils, simulation_structure
from qcore import shared as qc_shared
from qcore.qclogging import get_basic_logger, VERYVERBOSE, NOPRINTWARNING
from qcore.constants import (
    SimParams,
    FaultParams,
    RootParams,
    ROOT_DEFAULTS_FILE_NAME,
    PLATFORM_CONFIG,
    HF_DEFAULT_SEED,
    Components,
)
from workflow.automation.lib import shared
from workflow.automation.platform_config import platform_config

HF_VEL_MOD_1D = "hf_vel_mod_1d"


def install_simulation(
    version,
    sim_dir,
    rel_name,
    run_dir,
    vel_mod_dir,
    srf_file,
    stoch_file,
    stat_file_path,
    vs30_file_path,
    check_vm,
    fault_yaml_path,
    root_yaml_path,
    v1d_full_path,
    cybershake_root,
    site_specific=False,
    site_v1d_dir=None,
    sim_params_file=None,
    seed=HF_DEFAULT_SEED,
    logger: Logger = get_basic_logger(),
    extended_period=False,
    vm_perturbations=False,
    ignore_vm_perturbations=False,
    vm_qpqs_files=False,
    ignore_vm_qpqs_files=False,
    components=None,
):
    """Installs a single simulation"""

    lf_sim_root_dir = simulation_structure.get_lf_dir(sim_dir)
    hf_dir = simulation_structure.get_hf_dir(sim_dir)
    bb_dir = simulation_structure.get_bb_dir(sim_dir)
    im_calc_dir = simulation_structure.get_im_calc_dir(sim_dir)

    dir_list = [sim_dir, lf_sim_root_dir, hf_dir, bb_dir, im_calc_dir]
    version = str(version)
    if not os.path.isdir(cybershake_root):
        dir_list.insert(0, cybershake_root)

    shared.verify_user_dirs(dir_list)

    template_path = os.path.join(
        platform_config[PLATFORM_CONFIG.GMSIM_TEMPLATES_DIR.name], version
    )
    root_params_dict = utils.load_yaml(
        os.path.join(template_path, ROOT_DEFAULTS_FILE_NAME)
    )
    root_params_dict["ims"][RootParams.extended_period.value] = extended_period
    root_params_dict[RootParams.version.value] = version
    root_params_dict[RootParams.stat_file.value] = stat_file_path
    root_params_dict[RootParams.stat_vs_est.value] = vs30_file_path
    root_params_dict["hf"][RootParams.seed.value] = seed
    root_params_dict["hf"][HF_VEL_MOD_1D] = v1d_full_path
    if components is not None:
        if not set(components).issubset(set(Components.iterate_str_values())):
            message = f"{components} are not all in {Components}"
            logger.critical(message)
            raise ValueError(message)
        root_params_dict["ims"][RootParams.component.value] = components

    # Fault params
    fault_params_dict = {
        FaultParams.root_yaml_path.value: root_yaml_path,
        FaultParams.vel_mod_dir.value: vel_mod_dir,
    }
    fault_params_dict["hf"] = {
        FaultParams.site_specific.value: site_specific,
        FaultParams.site_v1d_dir.value: site_v1d_dir,
    }

    # VM params
    vm_params_path = simulation_structure.get_vm_params_yaml(vel_mod_dir)

    # Sim Params
    sim_params_dict = {
        SimParams.fault_yaml_path.value: fault_yaml_path,
        SimParams.run_name.value: rel_name,
        SimParams.user_root.value: cybershake_root,
        SimParams.run_dir.value: run_dir,
        SimParams.sim_dir.value: sim_dir,
        SimParams.srf_file.value: srf_file,
        SimParams.vm_params.value: vm_params_path,
    }

    if check_vm:
        vm_params_dict = utils.load_yaml(vm_params_path)
        dt = root_params_dict["dt"]
        sim_duration = vm_params_dict["sim_duration"]
        nt = float(sim_duration) / dt
        if not isclose(nt, round(nt)):
            logger.critical(
                "Simulation dt does not match sim duration. This will result in errors during BB. Simulation duration"
                " must be a multiple of dt. Ignoring fault. Simulation_duration: {}. dt: {}.".format(
                    sim_duration, dt
                )
            )
            return None, None, None

    sim_params_dict["emod3d"] = {}

    vm_pert_file = simulation_structure.get_realisation_VM_pert_file(
        cybershake_root, rel_name
    )
    if vm_perturbations:
        # We want to use the perturbation file
        if os.path.exists(vm_pert_file):
            # The perturbation file exists, use it
            root_params_dict["emod3d"]["model_style"] = 3
            sim_params_dict["emod3d"]["pertbfile"] = vm_pert_file
        else:
            # The perturbation file does not exist. Raise an exception
            message = f"The expected perturbation file {vm_pert_file} does not exist. Generate or move this file to the given location."
            logger.error(message)
            raise FileNotFoundError(message)
    elif not ignore_vm_perturbations and os.path.exists(vm_pert_file):
        # We haven't used either flag and the perturbation file exists. Raise an error and make the user deal with it
        message = f"The perturbation file {vm_pert_file} exists. Reset and run installation with the --ignore_vm_perturbations flag if you do not wish to use it."
        logger.error(message)
        raise FileExistsError(message)
    else:
        # The perturbation file doesn't exist or we are explicitly ignoring it. Keep going
        pass

    qsfile = simulation_structure.get_fault_qs_file(cybershake_root, rel_name)
    qpfile = simulation_structure.get_fault_qp_file(cybershake_root, rel_name)
    if vm_qpqs_files:
        # We want to use the Qp/Qs files
        if os.path.exists(qsfile) and os.path.exists(qpfile):
            # The Qp/Qs files exist, use them
            root_params_dict["emod3d"]["useqsqp"] = 1
            sim_params_dict["emod3d"]["qsfile"] = qsfile
            sim_params_dict["emod3d"]["qpfile"] = qpfile
        else:
            # At least one of the Qp/Qs files do not exist. Raise an exception
            message = f"The expected Qp/Qs files {qpfile} and/or {qsfile} do not exist. Generate or move these files to the given location."
            logger.error(message)
            raise FileExistsError(message)
    elif not ignore_vm_qpqs_files and (
        os.path.exists(qsfile) or os.path.exists(qpfile)
    ):
        # We haven't used either flag but the Qp/Qs files exist. Raise an error and make the user deal with it
        message = f"The Qp/Qs files {qpfile}, {qsfile}  exist. Reset and run installation with the --ignore_vm_qpqs_files flag if you do not wish to use them."
        logger.error(message)
        raise FileExistsError(message)
    else:
        # Either the Qp/Qs files don't exist, or we are explicitly ignoring them. Keep going
        pass

    sim_params_dict["hf"] = {SimParams.slip.value: stoch_file}
    sim_params_dict["bb"] = {}

    if sim_params_file is not None and os.path.isfile(sim_params_file):
        with open(sim_params_file) as spf:
            extra_sims_params = yaml.safe_load(spf)
        for key, value in extra_sims_params.items():
            # If the key exists in both dictionaries and maps to a dictionary in both, then merge them
            if (
                isinstance(value, dict)
                and key in sim_params_dict
                and isinstance(sim_params_dict[key], dict)
            ):
                sim_params_dict[key].update(value)
            else:
                sim_params_dict.update({key: value})

    return root_params_dict, fault_params_dict, sim_params_dict


def dump_all_yamls(sim_dir, root_params_dict, fault_params_dict, sim_params_dict):
    """Saves the yaml files at the specified locations"""
    utils.dump_yaml(sim_params_dict, os.path.join(sim_dir, "sim_params.yaml"))
    utils.dump_yaml(fault_params_dict, sim_params_dict["fault_yaml_path"])
    utils.dump_yaml(root_params_dict, fault_params_dict["root_yaml_path"])


def generate_fd_files(
    output_path,
    vm_params_dict,
    stat_file="default.ll",
    keep_dup_station=True,
    logger: Logger = get_basic_logger(),
):
    MODEL_LAT = vm_params_dict["MODEL_LAT"]
    MODEL_LON = vm_params_dict["MODEL_LON"]
    MODEL_ROT = vm_params_dict["MODEL_ROT"]
    hh = vm_params_dict["hh"]
    nx = vm_params_dict["nx"]
    ny = vm_params_dict["ny"]
    sufx = vm_params_dict["sufx"]
    shared.verify_strings([MODEL_LAT, MODEL_LON, MODEL_ROT, hh, nx, ny, sufx])

    filename = "fd{}".format(sufx)
    logger.info(stat_file)

    # arbitrary longlat station input
    ll_in = stat_file
    # where to save gridpoint and longlat station files
    gp_out = os.path.join(output_path, "{}.statcords".format(filename))
    ll_out = os.path.join(output_path, "{}.ll".format(filename))

    logger.info("From: {}. To: {}, {}".format(stat_file, gp_out, ll_out))

    # velocity model parameters
    nx = int(nx)
    ny = int(ny)
    mlat = float(MODEL_LAT)
    mlon = float(MODEL_LON)
    mrot = float(MODEL_ROT)
    hh = float(hh)

    # retrieve in station names, latitudes and longitudes
    sname, slat, slon = qc_shared.get_stations(ll_in, locations=True)
    slon = list(map(float, slon))
    slat = list(map(float, slat))

    # convert ll to grid points
    xy = geo.ll2gp_multi(
        list(map(list, zip(slon, slat))),
        mlon,
        mlat,
        mrot,
        nx,
        ny,
        hh,
        keep_outside=True,
    )

    # store gridpoints and names if unique position
    sxy = []
    suname = []
    for i in range(len(xy)):
        if xy[i] is None or xy[i][0] == nx - 1 or xy[i][1] == ny - 1:
            logger.log(VERYVERBOSE, "Station outside domain: {}".format(sname[i]))
        elif xy[i] not in sxy:
            sxy.append(xy[i])
            suname.append(sname[i])
        elif keep_dup_station:
            # still adds in the station but raise a warning
            sxy.append(xy[i])
            suname.append(sname[i])
            logger.log(
                NOPRINTWARNING, f"Duplicate Station added: {sname[i]} at {xy[i]}"
            )
        else:
            logger.log(VERYVERBOSE, "Duplicate Station Ignored: {}".format(sname[i]))

    # create grid point file
    with open(gp_out, "w") as gpf:
        # file starts with number of entries
        gpf.write("%d\n" % (len(sxy)))
        # x, y, z, name
        for i, xy in enumerate(sxy):
            gpf.write("%5d %5d %5d %s\n" % (xy[0], xy[1], 1, suname[i]))

    # convert unique grid points back to ll
    # warning: modifies sxy
    ll = geo.gp2ll_multi(sxy, mlat, mlon, mrot, nx, ny, hh)

    # create ll file
    with open(ll_out, "w") as llf:
        # lon, lat, name
        for i, pos in enumerate(ll):
            llf.write("%11.5f %11.5f %s\n" % (pos[0], pos[1], suname[i]))

    return gp_out, ll_out
