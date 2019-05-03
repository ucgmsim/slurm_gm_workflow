#!/usr/bin/env python3
"""Install a single cyberhake fault, NOT the whole cybershake run.

Called from inside a loop in install_cybershake.sh
"""

import os
import sys
import glob
import argparse

import qcore.simulation_structure as sim_struct
from qcore import utils, validate_vm, simulation_structure
from qcore.constants import FaultParams, ROOT_DEFAULTS_FILE_NAME, VM_PARAMS_FILE_NAME
from scripts.management import create_mgmt_db
from shared_workflow.install_shared import install_simulation, generate_fd_files, dump_all_yamls
from shared_workflow.shared_defaults import recipe_dir


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "path_cybershake",
        type=str,
        default=None,
        help="the path to the root of a specific version cybershake.",
    )
    parser.add_argument(
        "version", type=str, default="16.1", help="Please specify GMSim version"
    )
    parser.add_argument(
        "vm", type=str, default=None, help="the name of the Velocity Model."
    )
    parser.add_argument(
        "--n_rel", type=int, default=None, help="the number of realisations expected"
    )
    parser.add_argument(
        "--seed", type=int, default=0, help="The seed to be used for HF simulations"
    )

    args = parser.parse_args()

    install_fault(args.vm, args.n_rel, os.path.abspath(args.path_cybershake), args.version, args.seed)


def install_fault(fault_name, n_rel, root_folder, version, seed=0):

    config_dict = utils.load_yaml(os.path.join(recipe_dir, "gmsim", version, ROOT_DEFAULTS_FILE_NAME))
    # Load variables from cybershake config


    v1d_full_path = config_dict["v_1d_mod"]
    site_v1d_dir = config_dict.get("site_v1d_dir")
    hf_stat_vs_ref = config_dict.get("hf_stat_vs_ref")

    stat_file_path = config_dict['stat_file_path']
    vs30_file_path = stat_file_path.replace('.ll', '.vs30')
    vs30ref_file_path = stat_file_path.replace('.ll', '.vs30ref')

    error_log = os.path.join(root_folder, "install_error.log")

    # this variable has to be empty
    # TODO: fix this legacy issue, very low priority
    event_name = ""
    # Get & validate velocity model directory
    vel_mod_dir = simulation_structure.get_fault_VM_dir(root_folder, fault_name)
    valid_vm, message = validate_vm.validate_vm(vel_mod_dir)
    if not valid_vm:
        message = "Error: VM {} failed {}\n".format(fault_name, message)
        with open(error_log, "a") as error_fp:
            error_fp.write(message)
        raise RuntimeError(message)
    # Load the variables from vm_params.yaml
    vm_params_path = os.path.join(vel_mod_dir, VM_PARAMS_FILE_NAME)
    vm_params_dict = utils.load_yaml(vm_params_path)
    yes_model_params = (
        False
    )  # statgrid should normally be already generated with Velocity Model
    # get all srf from source

    srf_dir = simulation_structure.get_srf_dir(root_folder, fault_name)
    list_srf = glob.glob(os.path.join(srf_dir, "*.srf"))
    if n_rel is not None and len(list_srf) != n_rel:
        message = (
            "Error: fault {} failed. Number of realisations do "
            "not match number of SRF files\n".format(fault_name)
        )
        with open(error_log, "a") as error_fp:
            error_fp.write(message)
        raise RuntimeError(message)

    sim_root_dir = simulation_structure.get_runs_dir(root_folder)
    fault_yaml_path = simulation_structure.get_fault_yaml_path(sim_root_dir, fault_name)
    root_yaml_path = simulation_structure.get_root_yaml_path(sim_root_dir)
    for srf in list_srf:
        # try to match find the stoch with same basename
        srf_name = os.path.splitext(os.path.basename(srf))[0]
        stoch_file_path = simulation_structure.get_stoch_path(root_folder, srf_name)
        sim_params_file = simulation_structure.get_source_params_path(root_folder, srf_name)

        if not os.path.isfile(stoch_file_path):
            message = "Error: Corresponding Stoch file is not found:\n{}\n".format(
                stoch_file_path
            )
            print(message)
            with open(error_log, "a") as error_fp:
                error_fp.write(message)
            raise RuntimeError(message)

        # install pairs one by one to fit the new structure
        sim_dir = simulation_structure.get_sim_dir(root_folder, srf_name)
        root_params_dict, fault_params_dict, sim_params_dict, vm_add_params_dict = install_simulation(
            version=version,
            sim_dir=sim_dir,
            event_name=event_name,
            run_name=fault_name,
            run_dir=sim_root_dir,
            vel_mod_dir=vel_mod_dir,
            srf_file=srf,
            stoch_file=stoch_file_path,
            vm_params_path=vm_params_path,
            stat_file_path=stat_file_path,
            vs30_file_path=vs30_file_path,
            vs30ref_file_path=vs30ref_file_path,
            sufx=vm_params_dict['sufx'],
            sim_duration=vm_params_dict['sim_duration'],
            vel_mod_params_dir=vel_mod_dir,
            yes_statcords=False,
            yes_model_params=yes_model_params,
            fault_yaml_path=fault_yaml_path,
            root_yaml_path=root_yaml_path,
            user_root=root_folder,
            site_v1d_dir=site_v1d_dir,
            hf_stat_vs_ref=hf_stat_vs_ref,
            v1d_full_path=v1d_full_path,
            sim_params_file=sim_params_file,
            seed=seed,
        )

        vm_params_dict.update(vm_add_params_dict)

        create_mgmt_db.create_mgmt_db([], sim_struct.get_mgmt_db(root_folder), srf_files=srf)
        utils.setup_dir(os.path.join(root_folder, "mgmt_db_queue"))
        root_params_dict["mgmt_db_location"] = root_folder

        # Generate the fd files, create these at the fault level
        fd_statcords, fd_statlist = generate_fd_files(
            simulation_structure.get_fault_dir(root_folder, fault_name),
            vm_params_dict, stat_file=stat_file_path)

        fault_params_dict[FaultParams.stat_coords.value] = fd_statcords
        fault_params_dict[FaultParams.FD_STATLIST.value] = fd_statlist

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
