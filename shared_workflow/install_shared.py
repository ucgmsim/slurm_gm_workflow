"""Module that contains shared install functions"""
import os
import glob
from logging import Logger

from numpy import isclose
import yaml

import shared_workflow.shared_automated_workflow
import shared_workflow.shared_defaults as defaults
from qcore import geo, utils, simulation_structure
from qcore.constants import (
    SimParams,
    FaultParams,
    RootParams,
    VMParams,
    ROOT_DEFAULTS_FILE_NAME,
    HF_DEFAULT_SEED,
)
from shared_workflow import shared
from shared_workflow.workflow_logger import get_basic_logger, VERYVERBOSE


def install_simulation(
    version,
    sim_dir,
    event_name,
    run_name,
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
    v1d_dir=defaults.vel_mod_dir,
    user_root=defaults.user_root,
    stat_dir=defaults.stat_dir,
    site_v1d_dir=None,
    hf_stat_vs_ref=None,
    v1d_full_path=None,
    sim_params_file="",
    seed=HF_DEFAULT_SEED,
    logger: Logger = get_basic_logger(),
    extended_period=False,
):
    """Installs a single simulation"""
    lf_sim_root_dir = simulation_structure.get_lf_dir(sim_dir)
    hf_dir = simulation_structure.get_hf_dir(sim_dir)
    bb_dir = simulation_structure.get_bb_dir(sim_dir)
    im_calc_dir = simulation_structure.get_im_calc_dir(sim_dir)

    dir_list = [sim_dir, lf_sim_root_dir, hf_dir, bb_dir, im_calc_dir]
    version = str(version)
    if not os.path.isdir(user_root):
        dir_list.insert(0, user_root)

    shared.verify_user_dirs(dir_list)

    if not yes_model_params:
        logger.info(
            "Generation of model params has been skipped. Re-directing related params to files under {}".format(
                vel_mod_dir
            )
        )
        vel_mod_params_dir = vel_mod_dir

    if stat_file_path == "":
        # stat_path seems to empty, assigning all related value to latest_ll
        logger.info(
            "stat_file_path is not specified. Using {}".format(defaults.latest_ll)
        )
        run_stat_dir = os.path.join(stat_dir, event_name)
        stat_file_path = os.path.join(run_stat_dir, event_name + ".ll")
        vs30_file_path = os.path.join(run_stat_dir, event_name + ".vs30")
        vs30ref_file_path = os.path.join(run_stat_dir, event_name + ".vs30ref")

        # creating sub-folder for run_name
        # check if folder already exist
        if not os.path.isdir(run_stat_dir):
            # folder not exist, creating
            os.mkdir(run_stat_dir)

            # making symbolic link to latest_ll
            cmd = "ln -s {} {}".format(
                os.path.join(defaults.latest_ll_dir, defaults.latest_ll + ".ll"),
                stat_file_path,
            )
            shared_workflow.shared_automated_workflow.exe(cmd)

            # making symbolic link to lastest_ll.vs30 and .vs30ref
            cmd = "ln -s {} {}".format(
                os.path.join(defaults.latest_ll_dir, defaults.latest_ll + ".vs30"),
                vs30_file_path,
            )
            shared_workflow.shared_automated_workflow.exe(cmd)

            cmd = "ln -s {} {}".format(
                os.path.join(defaults.latest_ll_dir, defaults.latest_ll + ".vs30ref"),
                vs30ref_file_path,
            )
            shared_workflow.shared_automated_workflow.exe(cmd)

    template_path = os.path.join(defaults.recipe_dir, "gmsim", version)
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

    # VM params
    vm_params_dict = {
        VMParams.gridfile.value: os.path.join(
            vel_mod_params_dir, "gridfile{}".format(sufx)
        ),
        VMParams.gridout.value: os.path.join(
            vel_mod_params_dir, "gridout{}".format(sufx)
        ),
        VMParams.model_coords.value: os.path.join(
            vel_mod_params_dir, "model_coords{}".format(sufx)
        ),
        VMParams.model_params.value: os.path.join(
            vel_mod_params_dir, "model_params{}".format(sufx)
        ),
        VMParams.model_bounds.value: os.path.join(
            vel_mod_params_dir, "model_bounds{}".format(sufx)
        ),
    }

    # Sim Params
    sim_params_dict = {
        SimParams.fault_yaml_path.value: fault_yaml_path,
        SimParams.run_name.value: run_name,
        SimParams.user_root.value: user_root,
        SimParams.run_dir.value: run_dir,
        SimParams.sim_dir.value: sim_dir,
        SimParams.srf_file.value: srf_file,
        SimParams.vm_params.value: vm_params_path,
        SimParams.sim_duration.value: sim_duration,
    }
    if stat_file_path is not None:
        sim_params_dict[SimParams.stat_file.value] = stat_file_path

    nt = float(sim_duration) / root_params_dict['dt']
    if not isclose(nt, int(nt)):
        logger.critical(
            "Simulation dt does not match sim duration. This will result in errors during BB. Simulation duration must "
            "be a multiple of dt. Ignoring fault. Simulation_duration: {}. dt: {}.".format(
                sim_duration, root_params_dict['dt'])
        )
        return None, None, None, None

    sim_params_dict["emod3d"] = {}

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

    if sim_params_file and os.path.isfile(sim_params_file):
        with open(sim_params_file) as spf:
            extra_sims_params = yaml.load(spf)
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
    v1d_dir=defaults.vel_mod_dir,
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
        root_dict["bb"]["site_specific"] = False
        root_dict["v_mod_1d_name"] = v_mod_1d_selected

    # TODO:add in logic for site specific as well, if the user provided as args
    elif site_v1d_dir is not None and hf_stat_vs_ref is not None:
        v_mod_1d_path, hf_stat_vs_ref = shared.get_site_specific_path(
            os.path.dirname(stat_file),
            hf_stat_vs_ref=hf_stat_vs_ref,
            v1d_mod_dir=site_v1d_dir,
            logger=logger,
        )
        root_dict["bb"]["site_specific"] = True
        root_dict["v_mod_1d_name"] = v_mod_1d_path
        root_dict["hf_stat_vs_ref"] = hf_stat_vs_ref
    else:
        is_site_specific_id = q_site_specific()
        if is_site_specific_id:
            v_mod_1d_path, hf_stat_vs_ref = shared.get_site_specific_path(
                os.path.dirname(stat_file), logger=logger
            )
            root_dict["bb"]["site_specific"] = True
            root_dict["v_mod_1d_name"] = v_mod_1d_path
            root_dict["hf_stat_vs_ref"] = hf_stat_vs_ref
        else:
            v_mod_1d_name, v_mod_1d_selected = q_1d_velocity_model(v1d_dir)
            root_dict["bb"]["site_specific"] = False
            root_dict["v_mod_1d_name"] = v_mod_1d_selected


def q_1d_velocity_model(v_mod_1d_dir):
    shared.show_horizontal_line()
    print("Select one of 1D Velocity models (from %s)" % v_mod_1d_dir)
    shared.show_horizontal_line()

    v_mod_1d_options = glob.glob(os.path.join(v_mod_1d_dir, "*.1d"))
    v_mod_1d_options.sort()

    v_mod_1d_selected = shared.show_multiple_choice(v_mod_1d_options)
    print(v_mod_1d_selected)  # full path
    v_mod_1d_name = os.path.basename(v_mod_1d_selected).replace(".1d", "")

    return v_mod_1d_name, v_mod_1d_selected


def q_site_specific():
    shared.show_horizontal_line()
    print(
        "Do you want site-specific computation? "
        "(To use a universal 1D profile, Select 'No')"
    )
    shared.show_horizontal_line()
    return shared.show_yes_no_question()


def dump_all_yamls(
    sim_dir, root_params_dict, fault_params_dict, sim_params_dict, vm_params_dict
):
    """Saves the yaml files at the specified locations"""
    utils.dump_yaml(sim_params_dict, os.path.join(sim_dir, "sim_params.yaml"))
    utils.dump_yaml(fault_params_dict, sim_params_dict["fault_yaml_path"])
    utils.dump_yaml(root_params_dict, fault_params_dict["root_yaml_path"])
    utils.dump_yaml(
        vm_params_dict, os.path.join(fault_params_dict["vel_mod_dir"], "vm_params.yaml")
    )


def generate_fd_files(
    output_path,
    vm_params_dict,
    stat_file="default.ll",
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
        if xy[i] is None:
            logger.log(VERYVERBOSE, "Station outside domain: {}".format(sname[i]))
        elif xy[i] not in sxy:
            sxy.append(xy[i])
            suname.append(sname[i])
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
