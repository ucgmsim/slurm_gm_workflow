"""Module that contains shared install functions"""
import os
import glob
from logging import Logger

from numpy import isclose
import yaml

from qcore import geo, utils, simulation_structure
from qcore.qclogging import get_basic_logger, VERYVERBOSE, NOPRINTWARNING
from qcore.constants import (
    SimParams,
    FaultParams,
    RootParams,
    VMParams,
    ROOT_DEFAULTS_FILE_NAME,
    PLATFORM_CONFIG,
    HF_DEFAULT_SEED,
)
from shared_workflow import shared
from shared_workflow.platform_config import platform_config

HF_VEL_MOD_1D = "hf_vel_mod_1d"


def install_simulation(
    version,
    sim_dir,
    rel_name,
    run_dir,
    vel_mod_dir,
    srf_file,
    stoch_file,
    vm_params_path,
    stat_file_path,
    vs30_file_path,
    vs30ref_file_path,
    sufx,
    sim_duration,
    vel_mod_params_dir,
    yes_statcords,
    yes_model_params,
    fault_yaml_path,
    root_yaml_path,
    v1d_full_path,
    cybershake_root,
    v1d_dir=platform_config[PLATFORM_CONFIG.VELOCITY_MODEL_DIR.name],
    site_v1d_dir=None,
    hf_stat_vs_ref=None,
    sim_params_file=None,
    seed=HF_DEFAULT_SEED,
    logger: Logger = get_basic_logger(),
    extended_period=False,
    vm_perturbations=False,
    ignore_vm_perturbations=False,
    vm_qpqs_files=False,
    ignore_vm_qpqs_files=False,
):
    """Installs a single simulation"""
    run_name = simulation_structure.get_fault_from_realisation(rel_name)

    lf_sim_root_dir = simulation_structure.get_lf_dir(sim_dir)
    hf_dir = simulation_structure.get_hf_dir(sim_dir)
    bb_dir = simulation_structure.get_bb_dir(sim_dir)
    im_calc_dir = simulation_structure.get_im_calc_dir(sim_dir)

    dir_list = [sim_dir, lf_sim_root_dir, hf_dir, bb_dir, im_calc_dir]
    version = str(version)
    if not os.path.isdir(cybershake_root):
        dir_list.insert(0, cybershake_root)

    shared.verify_user_dirs(dir_list)

    if not yes_model_params:
        logger.info(
            "Generation of model params has been skipped. Re-directing related params to files under {}".format(
                vel_mod_dir
            )
        )
        vel_mod_params_dir = vel_mod_dir

    template_path = os.path.join(
        platform_config[PLATFORM_CONFIG.TEMPLATES_DIR.name], "gmsim", version
    )
    root_params_dict = utils.load_yaml(
        os.path.join(template_path, ROOT_DEFAULTS_FILE_NAME)
    )
    root_params_dict[RootParams.extended_period.value] = extended_period
    root_params_dict[RootParams.version.value] = version
    root_params_dict[RootParams.stat_file.value] = stat_file_path
    root_params_dict[RootParams.stat_vs_est.value] = vs30_file_path
    root_params_dict[RootParams.stat_vs_ref.value] = vs30ref_file_path
    root_params_dict["hf"][RootParams.seed.value] = seed

    # Fault params
    fault_params_dict = {
        FaultParams.root_yaml_path.value: root_yaml_path,
        FaultParams.vel_mod_dir.value: vel_mod_dir,
    }
    # read VM params
    vm_params_dict = load_yaml(vm_params_path) 
    # Sim Params
    sim_params_dict = {
        SimParams.fault_yaml_path.value: fault_yaml_path,
        SimParams.run_name.value: run_name,
        SimParams.user_root.value: cybershake_root,
        SimParams.run_dir.value: run_dir,
        SimParams.sim_dir.value: sim_dir,
        SimParams.srf_file.value: srf_file,
        SimParams.vm_params.value: vm_params_path,
        SimParams.sim_duration.value: sim_duration,
    }
    if stat_file_path is not None:
        sim_params_dict[SimParams.stat_file.value] = stat_file_path

    nt = float(sim_duration) / root_params_dict["dt"]
    if not isclose(nt, round(nt)):
        logger.critical(
            "Simulation dt does not match sim duration. This will result in errors during BB. Simulation duration must "
            "be a multiple of dt. Ignoring fault. Simulation_duration: {}. dt: {}.".format(
                sim_duration, root_params_dict["dt"]
            )
        )
        return None, None, None, None

    sim_params_dict["emod3d"] = {}

    vm_pert_file = simulation_structure.get_realisation_VM_pert_file(
        cybershake_root, rel_name
    )
    if vm_perturbations:
        # We want to use the perturbation file
        if os.path.exists(vm_pert_file):
            # The perturbation file exists, use it
            root_params_dict["emod3d"]["model_style"] = 3
            sim_params_dict["emod3d"]["pertb_file"] = vm_pert_file
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
            root_params_dict["emod3d"]["use_qpqs"] = 1
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

    shared.show_horizontal_line(c="*")

    if yes_statcords:
        logger.info("Producing statcords and FD_STATLIST. It may take a minute or two")

        fd_statcords, fd_statlist = generate_fd_files(
            sim_params_dict["sim_dir"],
            vm_params_dict,
            stat_file=stat_file_path,
            logger=logger,
        )
        logger.info("statcords and FD_STATLIST produced")
        fault_params_dict[FaultParams.stat_coords.value] = fd_statcords
        fault_params_dict[FaultParams.FD_STATLIST.value] = fd_statlist

    logger.info("installing bb")
    install_bb(
        stat_file_path,
        root_params_dict,
        v1d_dir=v1d_dir,
        v1d_full_path=v1d_full_path,
        site_v1d_dir=site_v1d_dir,
        hf_stat_vs_ref=hf_stat_vs_ref,
        logger=logger,
    )
    logger.info("installing bb finished")

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

    return root_params_dict, fault_params_dict, sim_params_dict, vm_params_dict


def install_bb(
    stat_file,
    root_dict,
    v1d_dir,
    v1d_full_path=None,
    site_v1d_dir=None,
    hf_stat_vs_ref=None,
    logger: Logger = get_basic_logger(),
):
    shared.show_horizontal_line(c="*")
    logger.info(" " * 37 + "EMOD3D HF/BB Preparation Ver.slurm")
    shared.show_horizontal_line(c="*")
    if v1d_full_path is not None:
        v_mod_1d_selected = v1d_full_path
        # temporary removed because master version of bb_sim does not take this as a argument
        # TODO: most of these logic are not required and should be removed
        # these logic are now depending on gmsim_version_template
        # root_dict["bb"]["site_specific"] = False
        root_dict["hf"][HF_VEL_MOD_1D] = v_mod_1d_selected

    # TODO:add in logic for site specific as well, if the user provided as args
    elif site_v1d_dir is not None and hf_stat_vs_ref is not None:
        hf_vel_mod_1d, hf_stat_vs_ref = shared.get_site_specific_path(
            os.path.dirname(stat_file),
            hf_stat_vs_ref=hf_stat_vs_ref,
            v1d_mod_dir=site_v1d_dir,
            logger=logger,
        )
        # root_dict["bb"]["site_specific"] = True
        root_dict["hf"][HF_VEL_MOD_1D] = hf_vel_mod_1d
        root_dict["hf_stat_vs_ref"] = hf_stat_vs_ref
    else:
        is_site_specific_id = q_site_specific()
        if is_site_specific_id:
            hf_vel_mod_1d, hf_stat_vs_ref = shared.get_site_specific_path(
                os.path.dirname(stat_file), logger=logger
            )
            # root_dict["bb"]["site_specific"] = True
            root_dict["hf"][HF_VEL_MOD_1D] = hf_vel_mod_1d
            root_dict["hf_stat_vs_ref"] = hf_stat_vs_ref
        else:
            hf_vel_mod_1d, v_mod_1d_selected = q_1d_velocity_model(v1d_dir)
            # root_dict["bb"]["site_specific"] = False
            root_dict["hf"][HF_VEL_MOD_1D] = v_mod_1d_selected


def q_1d_velocity_model(v_mod_1d_dir):
    shared.show_horizontal_line()
    print("Select one of 1D Velocity models (from %s)" % v_mod_1d_dir)
    shared.show_horizontal_line()

    v_mod_1d_options = glob.glob(os.path.join(v_mod_1d_dir, "*.1d"))
    v_mod_1d_options.sort()

    v_mod_1d_selected = shared.show_multiple_choice(v_mod_1d_options)
    print(v_mod_1d_selected)  # full path
    hf_vel_mod_1d = os.path.basename(v_mod_1d_selected).replace(".1d", "")

    return hf_vel_mod_1d, v_mod_1d_selected


def q_site_specific():
    shared.show_horizontal_line()
    print(
        "Do you want site-specific computation? "
        "(To use a universal 1D profile, Select 'No')"
    )
    shared.show_horizontal_line()
    return shared.show_yes_no_question()


def dump_all_yamls(
    sim_dir, root_params_dict, fault_params_dict, sim_params_dict
):
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
    sname, slat, slon = shared.get_stations(ll_in, locations=True)
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
