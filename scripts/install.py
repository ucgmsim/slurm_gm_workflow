#!/usr/bin/env python
"""
Creates the inital directory strcuture and files for a simulation

Usage example:
python ~/slurm_gm_workflow/scripts/install.py --user_root ~/Albury_newman
    --srf_dir ~/Albury_newman/Data/Sources/ --vm_dir ~/Albury/Data/VMs/
    --version gmsim_v18.5.3
"""
import os
import sys
import glob
import fnmatch
import configparser
import argparse
from datetime import datetime

import shared_workflow.shared_defaults as defaults
from qcore import utils, simulation_structure
from scripts.management import create_mgmt_db
from shared_workflow.install_shared import install_simulation, dump_all_yamls
from shared_workflow import shared

import pickle
import inspect

TEST_DATA_SAVE_DIR = "/nesi/nobackup/nesi00213/tmp/test_space/slurm_gm_workflow/pickled"
REALISATION = "PangopangoF29_HYP01-10_S1244"
DATA_TAKEN = {}


def q_accept_custom_rupmodel():
    shared.show_horizontal_line()
    print("Do you wish to use custom rupture files?")
    shared.show_horizontal_line()
    return shared.show_yes_no_question()


def q_custom_rupmodel_path():
    verified = False
    while not verified:
        rupture_path = input(
            "Enter path to custom Rupture Model (the parent of Srf/Stoch SRF directory): "
        )
        if not os.path.exists(rupture_path):
            print("Specified path doesn't exist. Try again")
            continue
        verified = True
    return rupture_path


def q_select_rupmodel_dir(mysrf_dir):
    func_name = 'q_select_rupmodel_dir'
    if not DATA_TAKEN.get(func_name):
        with open(os.path.join(TEST_DATA_SAVE_DIR, REALISATION, func_name + '_rupmodel_dir.P'), 'wb') as save_file:
            pickle.dump(mysrf_dir, save_file)

    shared.show_horizontal_line()
    print("Select Rupture Model - Step 1.")
    shared.show_horizontal_line()
    srf_options = os.listdir(mysrf_dir)
    srf_options = [
        x
        for x in srf_options
        if os.path.isdir(os.path.join(mysrf_dir, x)) and not x.startswith(".")
    ]  # avoid displaying .git
    srf_options.sort()
    if len(srf_options) == 0:  # this is a dead end
        print("No valid rupture model found")
        sys.exit()

    srf_selected = shared.show_multiple_choice(srf_options)
    print(srf_selected)
    srf_selected_dir = os.path.join(mysrf_dir, srf_selected, "Srf")
    try:
        srf_file_options = os.listdir(srf_selected_dir)
        # filter out files that are not *.srf
        srf_file_options = [x for x in srf_file_options if x.endswith(".srf")]
    except OSError:
        print("!! Srf directory not found. Going into %s" % srf_selected)
        return q_select_rupmodel_dir(os.path.join(mysrf_dir, srf_selected))

    if not DATA_TAKEN.get(func_name):
        with open(os.path.join(TEST_DATA_SAVE_DIR, REALISATION, func_name + '_srf_selected.P'), 'wb') as save_file:
            pickle.dump(srf_selected, save_file)
        with open(os.path.join(TEST_DATA_SAVE_DIR, REALISATION, func_name + '_srf_selected_dir.P'), 'wb') as save_file:
            pickle.dump(srf_selected_dir, save_file)
        with open(os.path.join(TEST_DATA_SAVE_DIR, REALISATION, func_name + '_srf_file_options.P'), 'wb') as save_file:
            pickle.dump(srf_file_options, save_file)

        DATA_TAKEN[func_name] = True

    return srf_selected, srf_selected_dir, srf_file_options


def q_select_rupmodel(srf_selected_dir, srf_file_options):
    frame = inspect.currentframe()
    args, _, _, values = inspect.getargvalues(frame)
    func_name = inspect.getframeinfo(frame)[2]
    if not DATA_TAKEN.get(func_name):
        for arg in args:
            with open(os.path.join(TEST_DATA_SAVE_DIR, REALISATION, func_name + '_{}.P'.format(arg)),
                      'wb') as save_file:
                pickle.dump(values[arg], save_file)

    """Rupture model user selection and verification"""
    shared.show_horizontal_line()
    print("Select Rupture Model - Step 2.")
    shared.show_horizontal_line()
    srf_file_options.sort()

    # choose one srf (instead of multiple to fit the new versioning
    # singular returns a str instead of str[]. reformat
    srf_file_selected = shared.show_multiple_choice(srf_file_options, singular=True)
    print(srf_file_selected)

    srf_file_path = os.path.join(srf_selected_dir, srf_file_selected)

    stoch_selected_dir = os.path.abspath(
        os.path.join(srf_selected_dir, os.path.pardir, "Stoch")
    )
    if not os.path.isdir(stoch_selected_dir):
        print(
            "Error: Corresponding Stoch directory "
            "is not found:\n%s" % stoch_selected_dir
        )
        sys.exit()

    stoch_file_path = os.path.join(stoch_selected_dir, srf_file_selected).replace(
        ".srf", ".stoch"
    )
    if not os.path.isfile(stoch_file_path):
        print("Error: Corresponding Stock " "file is not found:\n%s" % stoch_file_path)
        sys.exit()

    print("Corresponding Stock file is also found:\n%s" % stoch_file_path)

    if not DATA_TAKEN.get(func_name):
        with open(os.path.join(TEST_DATA_SAVE_DIR, REALISATION, func_name + '_srf_file_selected.P'), 'wb') as save_file:
            pickle.dump(srf_file_selected, save_file)
        with open(os.path.join(TEST_DATA_SAVE_DIR, REALISATION, func_name + '_srf_file_path.P'), 'wb') as save_file:
            pickle.dump(srf_file_path, save_file)
        with open(os.path.join(TEST_DATA_SAVE_DIR, REALISATION, func_name + '_stoch_file_path.P'), 'wb') as save_file:
            pickle.dump(stoch_file_path, save_file)

        DATA_TAKEN[func_name] = True

    return srf_file_selected, srf_file_path, stoch_file_path


def q_select_vel_model(vel_mod_dir):
    func_name = 'q_select_vel_model'
    if not DATA_TAKEN.get(func_name):
        with open(os.path.join(TEST_DATA_SAVE_DIR, REALISATION, func_name + '_vel_mod_dir.P'), 'wb') as save_file:
            pickle.dump(vel_mod_dir, save_file)
    """Velocity model user selection"""
    shared.show_horizontal_line()
    print("Select one of available VelocityModels (from %s)" % vel_mod_dir)
    shared.show_horizontal_line()

    v_mod_ver_options = []
    for root, dirnames, filenames in os.walk(vel_mod_dir):
        # returns the folder that contains params_vel.py
        for filename in fnmatch.filter(filenames, defaults.params_vel):
            v_mod_ver_options.append(root)

    v_mod_ver_options.sort()
    v_mod_ver = shared.show_multiple_choice(v_mod_ver_options)

    vel_mod_dir = os.path.join(vel_mod_dir, v_mod_ver)

    params_vel_path = os.path.join(vel_mod_dir, defaults.params_vel)
    if not os.path.exists(params_vel_path):
        print("Error: %s doesn't exist" % params_vel_path)
        sys.exit()

    if not DATA_TAKEN.get(func_name):
        with open(os.path.join(TEST_DATA_SAVE_DIR, REALISATION, func_name + '_v_mod_ver.P'), 'wb') as save_file:
            pickle.dump(v_mod_ver, save_file)
        with open(os.path.join(TEST_DATA_SAVE_DIR, REALISATION, func_name + '_vel_mod_dir.P'), 'wb') as save_file:
            pickle.dump(vel_mod_dir, save_file)
        with open(os.path.join(TEST_DATA_SAVE_DIR, REALISATION, func_name + '_params_vel_path.P'), 'wb') as save_file:
            pickle.dump(params_vel_path, save_file)

        DATA_TAKEN[func_name] = True


    return v_mod_ver, vel_mod_dir, params_vel_path


def q_real_stations():
    shared.show_horizontal_line()
    print("Do you wish to use real stations?")
    shared.show_horizontal_line()
    return shared.show_yes_no_question()


def q_select_stat_file(stat_dir, remove_fd=False):
    frame = inspect.currentframe()
    args, _, _, values = inspect.getargvalues(frame)
    func_name = inspect.getframeinfo(frame)[2]
    if not DATA_TAKEN.get(func_name):
        for arg in args:
            with open(os.path.join(TEST_DATA_SAVE_DIR, REALISATION, func_name + '_{}.P'.format(arg)),
                      'wb') as save_file:
                pickle.dump(values[arg], save_file)

    shared.show_horizontal_line()
    print("Select one of available Station list (from %s)" % stat_dir)
    shared.show_horizontal_line()
    stat_files = glob.glob(os.path.join(stat_dir, "*.ll"))
    stat_files = [os.path.basename(x) for x in stat_files]
    if remove_fd:
        stat_files = [x for x in stat_files if not x.startswith("fd_")]

    stat_files.sort()
    stat_file = shared.show_multiple_choice(stat_files)
    print(stat_file)
    stat_file_path = os.path.join(stat_dir, stat_file)
    print(stat_file_path)

    if not DATA_TAKEN.get(func_name):
        with open(os.path.join(TEST_DATA_SAVE_DIR, REALISATION, func_name + '_stat_file_path.P'), 'wb') as save_file:
            pickle.dump(stat_file_path, save_file)
        DATA_TAKEN[func_name] = True

    return stat_file_path


def q_select_vs30_file(stat_dir):
    frame = inspect.currentframe()
    args, _, _, values = inspect.getargvalues(frame)
    func_name = inspect.getframeinfo(frame)[2]
    if not DATA_TAKEN.get(func_name):
        for arg in args:
            with open(os.path.join(TEST_DATA_SAVE_DIR, REALISATION, func_name + '_{}.P'.format(arg)),
                      'wb') as save_file:
                pickle.dump(values[arg], save_file)

    shared.show_horizontal_line()
    print("Select one of available vs30 (from %s)" % stat_dir)
    shared.show_horizontal_line()
    vs30_files = glob.glob(os.path.join(stat_dir, "*.vs30"))
    vs30_files = [os.path.basename(x) for x in vs30_files]

    vs30_files.sort()
    vs30_file = shared.show_multiple_choice(vs30_files)
    print(vs30_file)
    vs30_file_path = os.path.join(stat_dir, vs30_file)
    print(vs30_file_path)
    vs30ref_file_path = vs30_file_path.replace(".vs30", ".vs30ref")
    if not os.path.isfile(vs30ref_file_path):
        print(
            "Error: Corresponding vs30ref " "file is not found:\n%s" % vs30ref_file_path
        )
        sys.exit()

    if not DATA_TAKEN.get(func_name):
        with open(os.path.join(TEST_DATA_SAVE_DIR, REALISATION, func_name + '_vs30_file_path.P'), 'wb') as save_file:
            pickle.dump(vs30_file_path, save_file)
        with open(os.path.join(TEST_DATA_SAVE_DIR, REALISATION, func_name + '_vs30ref_file_path.P'), 'wb') as save_file:
            pickle.dump(vs30ref_file_path, save_file)

        DATA_TAKEN[func_name] = True

    return vs30_file_path, vs30ref_file_path


def q_get_run_name(HH, srf_selected, v_mod_ver, emod3d_version):
    """Automatic generation of the run name (LP here only,
    HF and BB come later after declaration of HF and BB parameters).
    """
    frame = inspect.currentframe()
    args, _, _, values = inspect.getargvalues(frame)
    func_name = inspect.getframeinfo(frame)[2]
    if not DATA_TAKEN.get(func_name):
        for arg in args:
            with open(os.path.join(TEST_DATA_SAVE_DIR, REALISATION, func_name + '_{}.P'.format(arg)),
                      'wb') as save_file:
                pickle.dump(values[arg], save_file)

    # additional string to customize (today's date for starters)
    userString = datetime.now().strftime("%y%m%d")

    # use full name of RupModel directory #srf_selected.split("_")[0]
    srfString = srf_selected

    hString = "-h" + HH
    vModelString = "VM" + str(v_mod_ver)
    vString = "_EMODv" + emod3d_version

    # replace the decimal points with p
    # LPSim-2010Sept4_bev01_VMv1p64-h0p100_EMODv3p0p4_19May2016
    run_name = (
        (srfString + "_" + vModelString + hString + vString + "_" + userString)
        .replace(".", "p")
        .replace("/", "_")
    )

    yes = shared.confirm_name(run_name)

    if not DATA_TAKEN.get(func_name):
        with open(os.path.join(TEST_DATA_SAVE_DIR, REALISATION, func_name + '_yes.P'), 'wb') as save_file:
            pickle.dump(yes, save_file)
        with open(os.path.join(TEST_DATA_SAVE_DIR, REALISATION, func_name + '_run_name.P'), 'wb') as save_file:
            pickle.dump(run_name, save_file)

        DATA_TAKEN[func_name] = True

    return yes, run_name


def q_final_confirm(run_name, yes_statcords):
    print("")
    shared.show_horizontal_line(c="*")
    print("S U M M A R Y")
    shared.show_horizontal_line(c="*")
    print("")
    print("- Simulation directory: %s" % run_name)
    #    print "- Recipe to be copied from \n%s" %recipe_selected_dir
    print("- Statcoords generated: %s" % yes_statcords)
    # print "- Model Params generated: %s\n"%yes_model_params
    shared.show_horizontal_line(c="*")
    print("Do you wish to proceed?")
    return shared.show_yes_no_question()


def show_instruction(sim_dir):
    try:
        print(
            "Removing probably incomplete " + os.path.join(sim_dir, "params_base.pyc")
        )
        os.remove(os.path.join(sim_dir, "params_base.pyc"))
    except Exception as e:
        print(e.args)
        print("Could not remove params_base.pyc")

    shared.show_horizontal_line()
    print("Instructions")
    shared.show_horizontal_line()
    print("   For Simulation (Slurm based system)")
    print("    1.   cd %s" % sim_dir)
    print("    2.   Edit params.py and run_emod3d.sl.template as needed")
    print("    3.   python $gmsim/workflow/scripts/submit_emod3d.py")
    print("    4.   python $gmsim/workflow/scripts/submit_post_emod3d.py")
    print(
        "    5.   python $gmsim/workflow/scripts/submit_hf.py "
        "and python $gmsim/workflow/scripts/submit_bb.py"
    )
    print(
        "    Note: If you did not submit one or more .sl scripts, "
        "just do 'sbatch slurm_script.sl"
    )
    print("   For Plotting (hypocentre)")
    print(
        "    1.   Plotting station based data (IMs, Vs30, Obs PGA, pSA): "
        "plot_stations.py {datafile}.ll"
    )
    print(
        "    2.   Plotting timeslice-based data (PGV,MMI,animation): "
        "plot_transfer.py auto %s" % sim_dir
    )
    print("    3.   PGV,MMI: plot_ts_sum.py Timeslice sequence: plot_ts.py")


def q_wallclock():
    shared.show_horizontal_line()
    print("Do you wish to modify the wallclock time limit?")
    shared.show_horizontal_line()
    return shared.show_yes_no_question()


def main_local(args):
    shared.show_horizontal_line(c="*")
    print(" " * 37 + "EMOD3D Job Preparation Ver. Slurm")
    shared.show_horizontal_line(c="*")

    srf_selected, srf_selected_dir, srf_file_options = q_select_rupmodel_dir(
        args.srf_dir
    )
    srf_files_selected, srf_file, stoch_file = q_select_rupmodel(
        srf_selected_dir, srf_file_options
    )

    #    HH = q3() ## HH is taken directly from params_vel.py
    v_mod_ver, vel_mod_dir_full, params_vel_path = q_select_vel_model(args.vm_dir)

    with open(params_vel_path, "r") as f:
        exec(f.read(), globals())

    yes_real_stations = q_real_stations()
    if yes_real_stations:
        stat_file_path = q_select_stat_file(args.station_dir, remove_fd=True)
    else:
        stat_file_path = None

    vs30_file_path, vs30ref_file_path = q_select_vs30_file(args.station_dir)

    yes_flag, run_name = q_get_run_name(
        hh, srf_selected, v_mod_ver, defaults.emod3d_version
    )
    while True:
        run_name = shared.add_name_suffix(run_name, yes_flag)
        sim_dir = os.path.join(args.user_root, run_name)

        # sim_dir is new
        if not os.path.exists(sim_dir):
            os.mkdir(sim_dir)
            break

        print("Error: %s already exists. " "Add an additional suffix" % sim_dir)
        yes_flag = False

    # Always. If it becomes optional, params_base may have no entry
    # of stat_coords and FD_STATLIST
    yes_statcords = True

    # Always. To support statgrid, it is better to keep this way.
    yes_model_params = False

    final_yes = q_final_confirm(run_name, yes_statcords)

    if not final_yes:
        print("Installation exited")
        sys.exit()

    # we will keep model params in the same dir as sim_dir
    vel_mod_params_dir = sim_dir

    event_name = ""

    fault_yaml_path = simulation_structure.get_fault_yaml_path(sim_dir)
    root_yaml_path = simulation_structure.get_root_yaml_path(sim_dir)

    root_params_dict, fault_params_dict, sim_params_dict, vm_params_dict = install_simulation(
        args.version,
        sim_dir,
        event_name,
        run_name,
        defaults.run_dir,
        vel_mod_dir_full,
        srf_file,
        stoch_file,
        params_vel_path,
        stat_file_path,
        vs30_file_path,
        vs30ref_file_path,
        MODEL_LAT,
        MODEL_LON,
        MODEL_ROT,
        hh,
        nx,
        ny,
        nz,
        sufx,
        sim_duration,
        vel_mod_params_dir,
        yes_statcords,
        yes_model_params,
        fault_yaml_path,
        root_yaml_path,
        v1d_dir=args.v1d_dir,
        user_root=args.user_root,
        stat_dir=args.station_dir,
        site_v1d_dir=args.site_v1d_dir,
        hf_stat_vs_ref=args.hf_stat_vs_ref,
        v1d_full_path=args.v1d_full_path,
    )

    create_mgmt_db.create_mgmt_db([], sim_dir, srf_files=srf_file)
    utils.setup_dir(os.path.join(sim_dir, "mgmt_db_queue"))

    root_params_dict["mgmt_db_location"] = sim_dir

    dump_all_yamls(
        sim_dir, root_params_dict, fault_params_dict, sim_params_dict, vm_params_dict
    )

    print("Installation completed")
    show_instruction(sim_dir)


def main_remote(cfg, args):
    config = configparser.RawConfigParser()
    config.read(cfg)

    event_name = config.get("gmsim", "event_name")
    run_name = config.get("gmsim", "run_name")

    vel_mod_dir = config.get("remote", "vel_mod_dir")
    srf_file_path = config.get("remote", "srf_path")
    stat_file_path = config.get("remote", "stat_path")

    stoch_file_path = srf_file_path.replace("Srf", "Stoch").replace("srf", "stoch")
    vs30_file_path = stat_file_path.replace(".ll", ".vs30")
    vs30ref_file_path = stat_file_path.replace(".ll", ".vs30ref")

    srf_file = filter(None, (x.strip() for x in srf_file_path.splitlines()))
    stoch_file = filter(None, (x.strip() for x in stoch_file_path.splitlines()))

    print(srf_file, stoch_file)

    params_vel_path = os.path.join(vel_mod_dir, defaults.params_vel)
    with open(params_vel_path, "r") as f:
        exec(f.read(), globals())

    sim_dir = os.path.join(args.user_root, run_name)

    yes_statcords = True
    yes_model_params = False

    vel_mod_params_dir = vel_mod_dir

    fault_yaml_path = simulation_structure.get_fault_yaml_path(sim_dir)
    root_yaml_path = simulation_structure.get_root_yaml_path(sim_dir)

    # TODO action will return 4 params dict and they will be dumped into yamls.
    # TODO to implement when install_manual is merged
    root_params_dict, fault_params_dict, sim_params_dict, vm_params_dict = install_simulation(
        args.version,
        sim_dir,
        event_name,
        run_name,
        defaults.run_dir,
        vel_mod_dir,
        srf_file,
        stoch_file,
        params_vel_path,
        stat_file_path,
        vs30_file_path,
        vs30ref_file_path,
        MODEL_LAT,
        MODEL_LON,
        MODEL_ROT,
        hh,
        nx,
        ny,
        nz,
        sufx,
        sim_duration,
        vel_mod_params_dir,
        yes_statcords,
        yes_model_params,
        fault_yaml_path,
        root_yaml_path,
        v1d_dir=args.v1d_dir,
        user_root=args.user_root,
        stat_dir=args.station_dir,
        site_v1d_dir=args.site_v1d_dir,
        hf_stat_vs_ref=args.hf_stat_vs_ref,
        v1d_full_path=args.v1d_full_path,
    )

    utils.setup_dir(os.path.join(sim_dir, "mgmt_db_queue"))
    dump_all_yamls(
        sim_dir, root_params_dict, fault_params_dict, sim_params_dict, vm_params_dict
    )

    print("Installation completed")
    show_instruction(sim_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--user_root",
        type=str,
        default=defaults.user_root,
        help="the path to where to install the simulation",
    )
    parser.add_argument(
        "--sim_cfg",
        type=str,
        default=None,
        help="abs path to a file that contains all the config "
        "needed for a single sim install",
    )

    parser.add_argument(
        "--srf_dir",
        type=str,
        default=defaults.srf_default_dir,
        help="path that contains folders of faults/*.srf",
    )
    parser.add_argument(
        "--vm_dir",
        type=str,
        default=defaults.vel_mod_dir,
        help="path that contains VMs, params_vel must " "be present",
    )
    parser.add_argument(
        "--v1d_dir",
        type=str,
        default=defaults.v_mod_1d_dir,
        help="path pointing to the directory containing v1d file",
    )
    parser.add_argument(
        "--v1d_full_path",
        type=str,
        default=None,
        help="full path pointing to the generic v1d file",
    )
    parser.add_argument("--station_dir", type=str, default=defaults.stat_dir)
    parser.add_argument(
        "--version", type=str, default="16.1", help="version of simulation. eg.16.1"
    )
    parser.add_argument(
        "--site_v1d_dir",
        default=None,
        type=str,
        help="the to the directory containing site specific "
        "files, hf_stat_vs_ref must be provied as well if "
        "this is provided",
    )
    parser.add_argument(
        "--hf_stat_vs_ref",
        default=None,
        type=str,
        help="site_v1d_dir must be provied as well " "if this is provided",
    )

    args = parser.parse_args()

    # If the additional options provided, check if the folder exist
    for arg in vars(args):
        if arg != "version":
            path_to_check = getattr(args, arg)
            if path_to_check is not None:
                if not os.path.exists(path_to_check):
                    print("Error: path not exsist: %s" % path_to_check)
                    sys.exit()
                else:
                    print("%s is set to %s" % (arg, path_to_check))
            else:
                continue

    # change corresponding variables to the args provided

    # if sim_cfg parsed, run main_remote(which has no selection)
    if args.sim_cfg is not None:
        cfg = args.sim_cfg
        # check if the cfg exist, to prevent break
        if not os.path.exists(cfg):
            print("Error: No such file exists: %s" % cfg)
            sys.exit()
        else:
            main_remote(cfg, args)
    else:
        main_local(args)
