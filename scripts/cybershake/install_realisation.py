import argparse
import os
from datetime import datetime

from numpy import isclose
from qcore import utils
from qcore import simulation_structure as sim_struct
from qcore.constants import ROOT_DEFAULTS_FILE_NAME, HF_DEFAULT_SEED, TIMESTAMP_FORMAT
from qcore.utils import dump_yaml
from shared_workflow import workflow_logger
from shared_workflow.install_shared import (
    generate_root_params,
    generate_fault_params,
    generate_vm_params,
    generate_sim_params,
    generate_fd_files,
)
from shared_workflow.shared import verify_user_dirs
from shared_workflow.shared_defaults import recipe_dir
from shared_workflow.workflow_logger import get_basic_logger

INSTALL_LOG_FILE_NAME = "install_log_{}.txt"


def install_realisation(
    root_folder,
    rel_name,
    version,
    stat_file_path,
    extended_period,
    seed,
    install_logger=get_basic_logger(),
):
    sim_dir = sim_struct.get_sim_dir(root_folder, rel_name)

    lf_sim_root_dir = sim_struct.get_lf_dir(sim_dir)
    hf_dir = sim_struct.get_hf_dir(sim_dir)
    bb_dir = sim_struct.get_bb_dir(sim_dir)
    im_calc_dir = sim_struct.get_im_calc_dir(sim_dir)

    dir_list = [sim_dir, lf_sim_root_dir, hf_dir, bb_dir, im_calc_dir]
    verify_user_dirs(dir_list)

    fault_name = sim_struct.get_fault_from_realisation(rel_name)

    vm_params_path = sim_struct.get_VM_params_path(root_folder, rel_name)
    vm_params_dict = utils.load_yaml(vm_params_path)

    root_params = utils.load_yaml(
        os.path.join(recipe_dir, "gmsim", version, ROOT_DEFAULTS_FILE_NAME)
    )

    sim_duration = vm_params_dict["sim_duration"]
    dt = root_params["dt"]
    nt = float(sim_duration) / float(dt)
    if not isclose(nt, int(nt)):
        install_logger.critical(
            "Simulation dt does not match sim duration. This will result in errors during BB. Simulation duration must "
            "be a multiple of dt. Ignoring fault. Simulation_duration: {}. dt: {}.".format(
                sim_duration, dt
            )
        )
        return False

    root_yaml_path = sim_struct.get_root_yaml_path(sim_struct.get_runs_dir(root_folder))
    if not os.path.isfile(root_yaml_path):

        root_params = generate_root_params(
            root_params, root_folder, extended_period, seed, stat_file_path, version
        )
        dump_yaml(root_params, root_yaml_path)

    fault_yaml_path = sim_struct.get_fault_yaml_path(
        sim_struct.get_runs_dir(root_folder), fault_name
    )
    if not os.path.isfile(fault_yaml_path):
        fd_statcords, fd_statlist = generate_fd_files(
            sim_struct.get_fault_dir(root_folder, fault_name),
            vm_params_dict,
            stat_file=stat_file_path,
            logger=install_logger,
        )

        vel_mod_dir = sim_struct.get_fault_VM_dir(root_folder, fault_name)

        fault_params = generate_fault_params(
            root_folder, vel_mod_dir, fd_statcords, fd_statlist
        )
        dump_yaml(fault_params, fault_yaml_path)

        vm_params = generate_vm_params(vm_params_dict, vel_mod_dir)
        dump_yaml(vm_params, vm_params_path)

    sim_params_path = sim_struct.get_sim_yaml_path(
        sim_struct.get_runs_dir(root_folder), rel_name
    )
    sim_params = generate_sim_params(
        root_folder,
        rel_name,
        sim_dir,
        sim_duration,
        stat_file_path,
        logger=install_logger,
    )
    dump_yaml(sim_params, sim_params_path)
    return True


def main():
    install_logger = workflow_logger.get_logger()

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "path_cybershake",
        type=str,
        help="the path to the root of a specific version cybershake.",
    )
    parser.add_argument("realisation", type=str, help="The realisation to be installed")
    parser.add_argument("--version", type=str, default="16.1", help="The GMSim version")
    parser.add_argument(
        "--seed",
        type=str,
        default=HF_DEFAULT_SEED,
        help="The seed to be used for HF simulations. Default is to request a random seed.",
    )
    parser.add_argument(
        "--stat_file_path",
        type=str,
        default="/nesi/project/nesi00213/StationInfo/non_uniform_whole_nz_with_real_stations-hh400_v18p6p2.ll",
        help="The path to the station info file path.",
    )
    parser.add_argument(
        "--extended_period",
        action="store_true",
        help="Should IM_calc calculate more psa periods.",
    )

    args = parser.parse_args()

    path_cybershake = os.path.abspath(args.path_cybershake)
    realisation = args.realisation

    workflow_logger.add_general_file_handler(
        install_logger,
        os.path.join(
            sim_struct.get_sim_dir(path_cybershake, realisation),
            INSTALL_LOG_FILE_NAME.format(datetime.now().strftime(TIMESTAMP_FORMAT)),
        ),
    )

    install_realisation(
        path_cybershake,
        args.realisation,
        args.version,
        args.stat_file_path,
        args.extended_period,
        args.seed,
        install_logger,
    )


if __name__ == "__main__":
    main()
