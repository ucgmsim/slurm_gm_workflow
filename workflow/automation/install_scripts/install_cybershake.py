import argparse
from datetime import datetime
from logging import Logger
from pathlib import Path

from qcore import constants, qclogging, formats, simulation_structure, utils

from workflow.automation.install_scripts import create_mgmt_db
from workflow.automation.lib import constants as wf_constants
from workflow.automation.platform_config import platform_config

AUTO_SUBMIT_LOG_FILE_NAME = "install_cybershake_log_{}.txt"


def load_args(logger):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "cybershake_root",
        type=Path,
        help="the path to the root of a specific version cybershake.",
    )
    parser.add_argument(
        "fault_selection_list", type=Path, help="The fault selection file"
    )
    parser.add_argument(
        "version",
        type=str,
        help="Please specify GMSim version",
    )
    parser.add_argument(
        "--seed",
        type=str,
        default=constants.HF_DEFAULT_SEED,
        help="The seed to be used for HF simulations. Default is to request a random seed.",
    )
    parser.add_argument(
        "--stat_file_path",
        type=str,
        default="/nesi/project/nesi00213/StationInfo/non_uniform_whole_nz_with_real_stations-hh400_v20p3_land.ll",
        help="The path to the station info file path.",
    )
    parser.add_argument(
        "--components",
        choices=list(constants.Components.iterate_str_values()),
        nargs="+",
        default=None,
        help="list of components to run IM_calcs, overwrites value in selected gmsim version template",
    )
    parser.add_argument(
        "--extended_period",
        action="store_true",
        help="Should IM_calc calculate more psa periods.",
    )
    parser.add_argument(
        "--log_dir",
        type=Path,
        default=None,
        help="Location of the log file to use. Defaults to 'cybershake_log.txt' in the location root_folder. "
        "Must be absolute or relative to the root_folder.",
    )
    parser.add_argument(
        "--keep_dup_station",
        action="store_true",
        help="Keep stations if they snap to the same grid-point",
    )
    args = parser.parse_args()

    if args.log_dir is None:
        qclogging.add_general_file_handler(
            logger,
            args.cybershake_root
            / AUTO_SUBMIT_LOG_FILE_NAME.format(
                datetime.now().strftime(constants.TIMESTAMP_FORMAT)
            ),
        )
    else:
        qclogging.add_general_file_handler(
            logger,
            args.log_dir
            / AUTO_SUBMIT_LOG_FILE_NAME.format(
                datetime.now().strftime(constants.TIMESTAMP_FORMAT)
            ),
        )
    logger.debug("Added file handler to the logger")
    logger.debug(f"Arguments are as follows: {args}")

    messages = []

    gmsim_version_path = (
        Path(platform_config[constants.PLATFORM_CONFIG.GMSIM_TEMPLATES_DIR.name])
        / args.version
    )

    if gmsim_version_path.is_dir():
        for f_name in [constants.ROOT_DEFAULTS_FILE_NAME, "emod3d_defaults.yaml"]:
            if not (gmsim_version_path / f_name).exists():
                messages.append(
                    "Version {} does not have a required {} file in the directory {}".format(
                        args.version,
                        f_name,
                        gmsim_version_path,
                    )
                )
    else:
        messages.append(
            "Version {} does not exist, place a directory with that name into {}\n"
            "Also ensure it has contents of {} and {}".format(
                args.version,
                platform_config[constants.PLATFORM_CONFIG.GMSIM_TEMPLATES_DIR.name],
                constants.ROOT_DEFAULTS_FILE_NAME,
                "emod3d_defaults.yaml",
            )
        )

    if len(messages) > 0:
        message = "\n".join(messages)
        logger.error(message)
        parser.error(message)

    return args


def main():
    logger = qclogging.get_logger()
    args = load_args(logger)

    fault_selection = formats.load_fault_selection_file(args.fault_selection_list)

    cybershake_root = args.cybershake_root
    runs_dir = simulation_structure.get_runs_dir(cybershake_root)
    create_mgmt_db.create_mgmt_db(
        [],
        simulation_structure.get_mgmt_db(cybershake_root),
        fault_selection=fault_selection,
    )

    for fault, count in fault_selection.items():
        utils.setup_dir(simulation_structure.get_sim_dir(cybershake_root, fault))
        for i in range(1, count + 1):
            rel_name = simulation_structure.get_realisation_name(fault, i)
            utils.setup_dir(simulation_structure.get_sim_dir(cybershake_root, rel_name))

    utils.setup_dir(simulation_structure.get_mgmt_db_queue(cybershake_root))
    utils.setup_dir(runs_dir)

    root_params = generate_root_params(
        args.version,
        Path(args.stat_file_path).resolve(),
        cybershake_root,
        seed=args.seed,
        logger=logger,
        extended_period=args.extended_period,
        components=args.components,
        keep_dup_station=args.keep_dup_station,
    )

    root_params_path = simulation_structure.get_root_yaml_path(runs_dir)
    utils.dump_yaml(root_params, root_params_path)


def generate_root_params(
    version,
    stat_file_path,
    cybershake_root,
    seed=constants.HF_DEFAULT_SEED,
    logger: Logger = qclogging.get_basic_logger(),
    extended_period=False,
    components=None,
    keep_dup_station=True,
):
    template_path = (
        Path(platform_config[constants.PLATFORM_CONFIG.GMSIM_TEMPLATES_DIR.name])
        / version
    )

    root_params_dict = utils.load_yaml(
        template_path / constants.ROOT_DEFAULTS_FILE_NAME
    )

    vs30_file_path = stat_file_path.replace(".ll", ".vs30")
    v1d_full_path = (
        Path(platform_config[constants.PLATFORM_CONFIG.VELOCITY_MODEL_DIR.name])
        / "Mod-1D"
        / root_params_dict["v_1d_mod"]
    )

    root_params_dict.update(
        {
            constants.RootParams.version.value: version,
            constants.RootParams.stat_file.value: str(stat_file_path),
            constants.RootParams.stat_vs_est.value: str(vs30_file_path),
            constants.RootParams.keep_dup_station.value: keep_dup_station,
            constants.RootParams.mgmt_db_location.value: str(cybershake_root),
        }
    )

    root_params_dict["ims"][
        constants.RootParams.extended_period.value
    ] = extended_period
    root_params_dict["hf"][constants.RootParams.seed.value] = seed
    root_params_dict["hf"][wf_constants.HF_VEL_MOD_1D] = str(v1d_full_path)
    if components is not None:
        if not set(components).issubset(set(constants.Components.iterate_str_values())):
            message = f"{components} are not all in {constants.Components}"
            logger.critical(message)
            raise ValueError(message)
        root_params_dict["ims"][constants.RootParams.component.value] = components

    return root_params_dict


if __name__ == "__main__":
    main()
