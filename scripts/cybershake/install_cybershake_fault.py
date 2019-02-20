#!/usr/bin/env python3
"""Install a single cyberhake fault, NOT the whole cybershake run.

Called from inside a loop in install_cybershake.sh
"""

import os
import sys
import glob
import argparse

import shared_workflow.load_config as ldcfg
from qcore import utils, validate_vm, simulation_structure
from qcore.constants import FaultParams
from scripts.management import create_mgmt_db
from shared_workflow.install_shared import install_simulation, generate_fd_files, dump_all_yamls


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "path_cybershake",
        type=str,
        default=None,
        help="the path to the root of a specific version cybershake.",
    )
    parser.add_argument(
        "config",
        type=str,
        default=None,
        help="a path to a config file that constains all the required values.",
    )
    parser.add_argument(
        "vm", type=str, default=None, help="the name of the Velocity Model."
    )
    parser.add_argument(
        "--n_rel", type=int, default=None, help="the number of realisations expected"
    )

    args = parser.parse_args()
    try:
        # try to read the config file
        cybershake_cfg = ldcfg.load(
            os.path.dirname(args.config), os.path.basename(args.config)
        )
    except Exception as e:
        print(
            "While loading the configuration file the following error occurred: {}".format(
                e
            )
        )
        sys.exit()

    ldcfg.check_cfg_params_path(cybershake_cfg, "dt", "hf_dt", "version")

    # Load variables from cybershake config
    path_cybershake = args.path_cybershake
    sim_root_dir = os.path.join(path_cybershake, "Runs/")
    srf_root_dir = os.path.join(path_cybershake, "Data/Sources/")
    vm_root_dir = os.path.join(path_cybershake, "Data/VMs/")

    version = cybershake_cfg["version"]
    v1d_full_path = cybershake_cfg["v_1d_mod"]

    site_v1d_dir = cybershake_cfg.get("site_v1d_dir")
    hf_stat_vs_ref = cybershake_cfg.get("hf_stat_vs_ref")

    stat_file_path = cybershake_cfg['stat_file_path']
    vs30_file_path = stat_file_path.replace('.ll', '.vs30')
    vs30ref_file_path = stat_file_path.replace('.ll', '.vs30ref')

    error_log = os.path.join(path_cybershake, "install_error.log")
    params_vel = "params_vel.py"

    fault_name = args.vm

    # this variable has to be empty
    # TODO: fix this legacy issue, very low priority
    event_name = ""

    # Get & validate velocity model directory
    vel_mod_dir = os.path.join(vm_root_dir, fault_name)
    valid_vm, message = validate_vm.validate_vm(vel_mod_dir)
    if not valid_vm:
        message = "Error: VM {} failed {}".format(fault_name, message)
        print(message)
        with open(error_log, "a") as error_fp:
            error_fp.write(message)
        exit()

    # Load the variables from params_vel.py
    params_vel_path = os.path.join(vel_mod_dir, params_vel)
    with open(params_vel_path, "r") as f:
        exec(f.read(), globals())

    yes_model_params = (
        False
    )  # statgrid should normally be already generated with Velocity Model

    # get all srf from source
    srf_dir = os.path.join(os.path.join(srf_root_dir, fault_name), "Srf")
    stoch_dir = os.path.join(os.path.join(srf_root_dir, fault_name), "Stoch")
    sim_params_dir = os.path.join(os.path.join(srf_root_dir, fault_name), "Sim_params")
    list_srf = glob.glob(os.path.join(srf_dir, "*.srf"))

    if args.n_rel is not None and len(list_srf) != args.n_rel:
        message = (
            "Error: fault {} failed. Number of realisations do "
            "not match number of SRF files".format(fault_name)
        )
        print(message)
        with open(error_log, "a") as error_fp:
            error_fp.write(message)
        sys.exit()

    fault_yaml_path = simulation_structure.get_fault_yaml_path(sim_root_dir, fault_name)
    root_yaml_path = simulation_structure.get_root_yaml_path(sim_root_dir)

    for srf in list_srf:
        # try to match find the stoch with same basename
        srf_name = os.path.splitext(os.path.basename(srf))[0]
        stoch_file_path = os.path.join(stoch_dir, srf_name + ".stoch")
        sim_params_file = os.path.join(sim_params_dir, srf_name + ".yaml")

        if not os.path.isfile(stoch_file_path):
            message = "Error: Corresponding Stoch file is not found:\n{}".format(
                stoch_file_path
            )
            print(message)
            with open(error_log, "a") as error_fp:
                error_fp.write(message)
            sys.exit()
        else:
            # install pairs one by one to fit the new structure
            sim_dir = os.path.join(os.path.join(sim_root_dir, fault_name), srf_name)
            root_params_dict, fault_params_dict, sim_params_dict, vm_params_dict = install_simulation(
                version=version,
                sim_dir=sim_dir,
                event_name=event_name,
                run_name=fault_name,
                run_dir=sim_root_dir,
                vel_mod_dir=vel_mod_dir,
                srf_file=srf,
                stoch_file=stoch_file_path,
                params_vel_path=params_vel_path,
                stat_file_path=stat_file_path,
                vs30_file_path=vs30_file_path,
                vs30ref_file_path=vs30ref_file_path,
                MODEL_LAT=MODEL_LAT,
                MODEL_LON=MODEL_LON,
                MODEL_ROT=MODEL_ROT,
                hh=hh,
                nx=nx,
                ny=ny,
                nz=nz,
                sufx=sufx,
                sim_duration=sim_duration,
                vel_mod_params_dir=vel_mod_dir,
                yes_statcords=False,
                yes_model_params=yes_model_params,
                fault_yaml_path=fault_yaml_path,
                root_yaml_path=root_yaml_path,
                user_root=path_cybershake,
                site_v1d_dir=site_v1d_dir,
                hf_stat_vs_ref=hf_stat_vs_ref,
                v1d_full_path=v1d_full_path,
                sim_params_file=sim_params_file,
            )

            create_mgmt_db.create_mgmt_db([], path_cybershake, srf_files=srf)
            utils.setup_dir(os.path.join(path_cybershake, "mgmt_db_queue"))
            root_params_dict["mgmt_db_location"] = path_cybershake

            # Generate the fd files, create these at the fault level
            fd_statcords, fd_statlist = generate_fd_files(
                simulation_structure.get_fault_dir(path_cybershake, fault_name),
                vm_params_dict, stat_file=stat_file_path)

            fault_params_dict[FaultParams.stat_coords.value] = fd_statcords
            fault_params_dict[FaultParams.FD_STATLIST.value] = fd_statlist

            # store extra params provided. This step has been moved into install_bb function
            # if 'hf_stat_vs_ref' in cybershake_cfg:
            #     root_params_dict['hf_stat_vs_ref'] = cybershake_cfg['hf_stat_vs_ref']
            dump_all_yamls(
                sim_dir,
                root_params_dict,
                fault_params_dict,
                sim_params_dict,
                vm_params_dict,
            )


if __name__ == "__main__":
    main()
