import argparse
from pathlib import Path

from qcore import constants, simulation_structure, utils, qclogging


def load_args():
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("cs_root", type=Path)
    parser.add_argument("realisation")
    parser.add_argument("--log_dir", type=Path)

    vm_pert = parser.add_mutually_exclusive_group()
    vm_pert.add_argument(
        "--vm_perturbations",
        action="store_true",
        help="Use velocity model perturbations. If this is selected all realisations must have a perturbation file.",
    )
    vm_pert.add_argument(
        "--ignore_vm_perturbations",
        action="store_true",
        help="Don't use velocity model perturbations. If this is selected any perturbation files will be ignored.",
    )

    qp_qs = parser.add_mutually_exclusive_group()
    qp_qs.add_argument(
        "--vm_qpqs_files",
        action="store_true",
        help="Use generated Qp/Qs files. If this is selected all events/faults must have Qp and Qs files.",
    )
    qp_qs.add_argument(
        "--ignore_vm_qpqs_files",
        action="store_true",
        help="Don't use generated Qp/Qs files. If this is selected any Qp/Qs files will be ignored.",
    )

    args = parser.parse_args()
    return args


def generate_sim_params_yaml(
    rel_name,
    cybershake_root,
    vm_perturbations=False,
    ignore_vm_perturbations=False,
    vm_qpqs_files=False,
    ignore_vm_qpqs_files=False,
    logger=qclogging.get_basic_logger(),
):

    fault_name = simulation_structure.get_fault_from_realisation(rel_name)
    rel_sim_dir = Path(simulation_structure.get_sim_dir(cybershake_root, rel_name))
    source_params_path = Path(
        simulation_structure.get_sources_dir(cybershake_root)
    ) / simulation_structure.get_source_params_location(rel_name)

    runs_dir = simulation_structure.get_runs_dir(cybershake_root)
    fault_yaml_path = simulation_structure.get_fault_yaml_path(runs_dir, fault_name)
    vm_params_path = simulation_structure.get_vm_params_yaml(
        simulation_structure.get_fault_VM_dir(cybershake_root, fault_name)
    )
    srf_file = simulation_structure.get_srf_path(cybershake_root, rel_name)
    stoch_file = simulation_structure.get_stoch_path(cybershake_root, rel_name)

    sim_params_dict = {
        constants.SimParams.user_root.value: str(cybershake_root),
        constants.SimParams.run_dir.value: runs_dir,
        constants.SimParams.fault_yaml_path.value: fault_yaml_path,
        constants.SimParams.sim_dir.value: str(rel_sim_dir),
        constants.SimParams.run_name.value: rel_name,
        constants.SimParams.srf_file.value: srf_file,
        constants.SimParams.vm_params.value: vm_params_path,
        "emod3d": {},
        "hf": {constants.SimParams.slip.value: stoch_file},
        "bb": {},
    }

    vm_pert_file = Path(
        simulation_structure.get_realisation_VM_pert_file(cybershake_root, rel_name)
    )
    if vm_perturbations:
        # We want to use the perturbation file
        if vm_pert_file.is_file():
            # The perturbation file exists, use it
            sim_params_dict["emod3d"]["model_style"] = 3
            sim_params_dict["emod3d"]["pertbfile"] = str(vm_pert_file)
        else:
            # The perturbation file does not exist. Raise an exception
            message = f"The expected perturbation file {vm_pert_file} does not exist. Generate or move this file to the given location."
            logger.error(message)
            raise FileNotFoundError(message)
    elif not ignore_vm_perturbations and vm_pert_file.is_file():
        # We haven't used either flag and the perturbation file exists. Raise an error and make the user deal with it
        message = f"The perturbation file {vm_pert_file} exists. Reset and run installation with the --ignore_vm_perturbations flag if you do not wish to use it."
        logger.error(message)
        raise FileExistsError(message)
    else:
        # The perturbation file doesn't exist or we are explicitly ignoring it. Keep going
        pass

    qsfile = Path(simulation_structure.get_fault_qs_file(cybershake_root, rel_name))
    qpfile = Path(simulation_structure.get_fault_qp_file(cybershake_root, rel_name))
    if vm_qpqs_files:
        # We want to use the Qp/Qs files
        if qsfile.is_file() and qpfile.is_file():
            # The Qp/Qs files exist, use them
            sim_params_dict["emod3d"]["useqsqp"] = 1
            sim_params_dict["emod3d"]["qsfile"] = str(qsfile)
            sim_params_dict["emod3d"]["qpfile"] = str(qpfile)
        else:
            # At least one of the Qp/Qs files do not exist. Raise an exception
            message = f"The expected Qp/Qs files {qpfile} and/or {qsfile} do not exist. Generate or move these files to the given location."
            logger.error(message)
            raise FileExistsError(message)
    elif not ignore_vm_qpqs_files and (qsfile.is_file() or qpfile.is_file()):
        # We haven't used either flag but the Qp/Qs files exist. Raise an error and make the user deal with it
        message = f"The Qp/Qs files {qpfile}, {qsfile}  exist. Reset and run installation with the --ignore_vm_qpqs_files flag if you do not wish to use them."
        logger.error(message)
        raise FileExistsError(message)
    else:
        # Either the Qp/Qs files don't exist, or we are explicitly ignoring them. Keep going
        pass

    if source_params_path.is_file():
        source_params = utils.load_yaml(source_params_path)
        for key, value in source_params.items():
            # If the key exists in both dictionaries and maps to a dictionary in both, then merge them
            if (
                isinstance(value, dict)
                and key in sim_params_dict
                and isinstance(sim_params_dict[key], dict)
            ):
                sim_params_dict[key].update(value)
            else:
                sim_params_dict.update({key: value})
    return sim_params_dict


def main():
    args = load_args()

    cybershake_root = args.cs_root
    rel_name = args.realisation
    log_dir = args.log_dir
    if log_dir is None:
        log_dir = Path(simulation_structure.get_sim_dir(cybershake_root, rel_name))
    logger = qclogging.get_logger(f"install_rel_{rel_name}")
    qclogging.add_general_file_handler(logger, log_dir / f"install_rel_{rel_name}")

    rel_sim_dir = Path(simulation_structure.get_sim_dir(cybershake_root, rel_name))

    lf_sim_root_dir = simulation_structure.get_lf_dir(rel_sim_dir)
    hf_dir = simulation_structure.get_hf_dir(rel_sim_dir)
    bb_dir = simulation_structure.get_bb_dir(rel_sim_dir)
    im_calc_dir = simulation_structure.get_im_calc_dir(rel_sim_dir)

    dir_list = [rel_sim_dir, lf_sim_root_dir, hf_dir, bb_dir, im_calc_dir]
    for dir in dir_list:
        utils.setup_dir(dir)

    sim_params_dict = generate_sim_params_yaml(
        rel_name,
        cybershake_root,
        args.vm_perturbations,
        args.ignore_vm_perturbations,
        args.vm_qpqs_files,
        args.ignore_vm_qpqs_files,
        logger=logger,
    )

    sim_params_path = Path(simulation_structure.get_sim_params_yaml_path(rel_sim_dir))
    utils.dump_yaml(sim_params_dict, sim_params_path)


if __name__ == "__main__":
    main()
