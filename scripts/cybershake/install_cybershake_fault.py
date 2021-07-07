#!/usr/bin/env python3
"""Install a single cyberhake fault, NOT the whole cybershake run.

Called from inside a loop in install_cybershake.sh
"""

import os
import sys
import glob
import argparse
from logging import Logger
import shlex

from scripts.bb_sim import args_parser as bb_args_parser
from scripts.hf_sim import args_parser as hf_args_parser
from scripts.submit_bb import gen_command_template as bb_gen_command_template
from scripts.submit_hf import gen_command_template as hf_gen_command_template

from numpy import isclose
from qcore import utils, validate_vm, simulation_structure
from qcore.constants import (
    FaultParams,
    ROOT_DEFAULTS_FILE_NAME,
    ProcessType,
    PLATFORM_CONFIG,
    HF_DEFAULT_SEED,
    VM_PARAMS_FILE_NAME,
)
from qcore.qclogging import get_basic_logger, NOPRINTCRITICAL

from scripts.management import create_mgmt_db
from shared_workflow.install_shared import (
    install_simulation,
    generate_fd_files,
    dump_all_yamls,
)

# from shared_workflow.shared_template import generate_command
from shared_workflow.platform_config import platform_config, HPC


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
        "--seed",
        type=int,
        default=HF_DEFAULT_SEED,
        help="The seed to be used for HF simulations",
    )

    args = parser.parse_args()

    install_fault(
        args.vm,
        args.n_rel,
        os.path.abspath(args.path_cybershake),
        args.version,
        args.seed,
    )


def gen_args_cmd(command_template, template_parameters, add_args={}):
    # this function is adapted from generate_command from shared_workflow.shared_template
    command_parts = command_template.format(**template_parameters).split()
    command_parts_copy = list(command_parts)
    # remove srun, python, and *.py from the command
    for i in command_parts:
        if i in ["srun", "mpirun", "ibrun", "python"] or i.endswith(".py"):
            command_parts_copy.remove(i)
    command_parts = command_parts_copy

    for key in add_args:
        command_parts.append("--" + key)
        if add_args[key] is True:
            continue
        argument = str(add_args[key])

        for arg in shlex.split(argument):
            command_parts.append(arg)

    return list(map(str, command_parts))


def install_fault(
    fault_name,
    n_rel,
    root_folder,
    version,
    stat_file_path,
    seed=HF_DEFAULT_SEED,
    extended_period=False,
    vm_perturbations=False,
    ignore_vm_perturbations=False,
    vm_qpqs_files=False,
    ignore_vm_qpqs_files=False,
    keep_dup_station=True,
    components=None,
    logger: Logger = get_basic_logger(),
):

    config_dict = utils.load_yaml(
        os.path.join(
            platform_config[PLATFORM_CONFIG.TEMPLATES_DIR.name],
            "gmsim",
            version,
            ROOT_DEFAULTS_FILE_NAME,
        )
    )
    # Load variables from cybershake config

    v1d_full_path = os.path.join(
        platform_config[PLATFORM_CONFIG.VELOCITY_MODEL_DIR.name],
        "Mod-1D",
        config_dict.get("v_1d_mod"),
    )
    site_vm_dir = config_dict.get("site_vm_dir")
    hf_stat_vs_ref = config_dict.get("hf_stat_vs_ref")

    vs30_file_path = stat_file_path.replace(".ll", ".vs30")
    vs30ref_file_path = stat_file_path.replace(".ll", ".vs30ref")

    # this variable has to be empty
    # TODO: fix this legacy issue, very low priority
    event_name = ""

    # get all srf from source
    srf_dir = simulation_structure.get_srf_dir(root_folder, fault_name)

    list_srf = glob.glob(os.path.join(srf_dir, "*_REL*.srf"))
    if len(list_srf) == 0:
        list_srf = glob.glob(os.path.join(srf_dir, "*.srf"))

    list_srf.sort()
    if n_rel is not None and len(list_srf) != n_rel:
        message = (
            "Error: fault {} failed. Number of realisations do "
            "not match number of SRF files".format(fault_name)
        )
        logger.log(NOPRINTCRITICAL, message)
        raise RuntimeError(message)

    # Get & validate velocity model directory
    vel_mod_dir = simulation_structure.get_fault_VM_dir(root_folder, fault_name)
    valid_vm, message = validate_vm.validate_vm(vel_mod_dir, srf=list_srf[0])
    if not valid_vm:
        message = "Error: VM {} failed {}".format(fault_name, message)
        logger.log(NOPRINTCRITICAL, message)
        raise RuntimeError(message)
    # Load the variables from vm_params.yaml
    vm_params_path = os.path.join(vel_mod_dir, VM_PARAMS_FILE_NAME)
    vm_params_dict = utils.load_yaml(vm_params_path)
    yes_model_params = (
        False  # statgrid should normally be already generated with Velocity Model
    )

    sim_root_dir = simulation_structure.get_runs_dir(root_folder)
    fault_yaml_path = simulation_structure.get_fault_yaml_path(sim_root_dir, fault_name)
    root_yaml_path = simulation_structure.get_root_yaml_path(sim_root_dir)
    for srf in list_srf:
        logger.info("Installing {}".format(srf))
        # try to match find the stoch with same basename
        realisation_name = os.path.splitext(os.path.basename(srf))[0]
        stoch_file_path = simulation_structure.get_stoch_path(
            root_folder, realisation_name
        )
        sim_params_file = simulation_structure.get_source_params_path(
            root_folder, realisation_name
        )

        if not os.path.isfile(stoch_file_path):
            message = "Error: Corresponding Stoch file is not found: {}".format(
                stoch_file_path
            )
            logger.log(NOPRINTCRITICAL, message)
            raise RuntimeError(message)

        # install pairs one by one to fit the new structure
        sim_dir = simulation_structure.get_sim_dir(root_folder, realisation_name)

        (root_params_dict, fault_params_dict, sim_params_dict) = install_simulation(
            version=version,
            sim_dir=sim_dir,
            rel_name=realisation_name,
            run_dir=sim_root_dir,
            vel_mod_dir=vel_mod_dir,
            srf_file=srf,
            stoch_file=stoch_file_path,
            stat_file_path=stat_file_path,
            vs30_file_path=vs30_file_path,
            vs30ref_file_path=vs30ref_file_path,
            yes_statcords=False,
            fault_yaml_path=fault_yaml_path,
            root_yaml_path=root_yaml_path,
            cybershake_root=root_folder,
            site_vm_dir=site_vm_dir,
            hf_stat_vs_ref=hf_stat_vs_ref,
            v1d_full_path=v1d_full_path,
            sim_params_file=sim_params_file,
            seed=seed,
            logger=logger,
            extended_period=extended_period,
            vm_perturbations=vm_perturbations,
            ignore_vm_perturbations=ignore_vm_perturbations,
            vm_qpqs_files=vm_qpqs_files,
            ignore_vm_qpqs_files=ignore_vm_qpqs_files,
            components=components,
        )

        if (
            root_params_dict is None
            or fault_params_dict is None
            or sim_params_dict is None
        ):
            # Something has gone wrong, returning without saving anything
            logger.critical(f"Critical Error some params dictionary are None")
            return

        if root_params_dict is not None and not isclose(
            vm_params_dict["flo"], root_params_dict["flo"]
        ):
            logger.critical(
                "The parameter 'flo' does not match in the VM params and root params files. "
                "Please ensure you are installing the correct gmsim version"
            )
            return

        create_mgmt_db.create_mgmt_db(
            [], simulation_structure.get_mgmt_db(root_folder), srf_files=srf
        )
        utils.setup_dir(os.path.join(root_folder, "mgmt_db_queue"))
        root_params_dict["mgmt_db_location"] = root_folder

        # Generate the fd files, create these at the fault level
        fd_statcords, fd_statlist = generate_fd_files(
            simulation_structure.get_fault_dir(root_folder, fault_name),
            vm_params_dict,
            stat_file=stat_file_path,
            logger=logger,
            keep_dup_station=keep_dup_station,
        )

        fault_params_dict[FaultParams.stat_coords.value] = fd_statcords
        fault_params_dict[FaultParams.FD_STATLIST.value] = fd_statlist

        #     root_params_dict['hf_stat_vs_ref'] = cybershake_cfg['hf_stat_vs_ref']
        dump_all_yamls(sim_dir, root_params_dict, fault_params_dict, sim_params_dict)

        # test if the params are accepted by steps HF and BB
        sim_params = utils.load_sim_params(os.path.join(sim_dir, "sim_params.yaml"))
        # check hf

        # temporary change the script name to hf_sim, due to how error message are shown
        main_script_name = sys.argv[0]
        sys.argv[0] = "hf_sim.py"

        command_template, add_args = hf_gen_command_template(
            sim_params, list(HPC)[0].name, seed
        )
        run_command = gen_args_cmd(
            ProcessType.HF.command_template, command_template, add_args
        )
        hf_args_parser(cmd=run_command)

        # check bb
        sys.argv[0] = "bb_sim.py"

        command_template, add_args = bb_gen_command_template(sim_params)
        run_command = gen_args_cmd(
            ProcessType.BB.command_template, command_template, add_args
        )
        bb_args_parser(cmd=run_command)
        # change back, to prevent unexpected error
        sys.argv[0] = main_script_name


if __name__ == "__main__":
    main()
