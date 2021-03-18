from logging import Logger
import numpy as np
from os import path

from qcore import constants as const
from qcore import utils
from qcore.formats import load_station_file
from qcore.qclogging import get_basic_logger
from qcore import simulation_structure as sim_struct

from estimation.estimate_wct import EstModel, est_IM_chours_single
from shared_workflow.platform_config import (
    platform_config,
    get_platform_node_requirements,
    get_target_machine,
)
from shared_workflow.shared import set_wct
from shared_workflow.shared_automated_workflow import submit_script_to_scheduler
from shared_workflow.shared_template import write_sl_script


def submit_im_calc_slurm(
    sim_dir: str,
    write_dir: str = None,
    simple_out: bool = True,
    adv_ims: bool = False,
    target_machine: str = get_target_machine(const.ProcessType.IM_calculation).name,
    est_model: EstModel = path.join(
        platform_config[const.PLATFORM_CONFIG.ESTIMATION_MODELS_DIR.name], "IM"
    ),
    logger: Logger = get_basic_logger(),
):
    """Creates the IM calc slurm scrip, also submits if specified

    The options_dict is populated by the DEFAULT_OPTIONS, values can be changed by
    passing in a dict containing the entries that require changing. Merges the
    two dictionaries, the passed in one has higher priority.
    """
    # Load the yaml params
    params = utils.load_sim_params(sim_struct.get_sim_params_yaml_path(sim_dir), load_vm=False)
    realisation_name = params["name"]
    fault_name = sim_struct.get_fault_from_realisation(realisation_name)
    station_count = len(load_station_file(params["FD_STATLIST"]).index)

    header_options = {
        const.SlHdrOptConsts.description.value: "Calculates intensity measures.",
        const.SlHdrOptConsts.additional.value: ["#SBATCH --hint=nomultithread"],
        const.SlHdrOptConsts.memory.value: "2G",
        const.SlHdrOptConsts.version.value: "slurm",
        "exe_time": const.timestamp,
    }

    body_options = {
        const.SlBodyOptConsts.component.value: "",
        "realisation_name": realisation_name,
        const.SlBodyOptConsts.fault_name.value: fault_name,
        "np": platform_config[const.PLATFORM_CONFIG.IM_CALC_DEFAULT_N_CORES.name],
        "output_csv": sim_struct.get_IM_csv(sim_dir),
        "output_info": sim_struct.get_IM_info(sim_dir),
        "models": "",
        const.SlBodyOptConsts.mgmt_db.value: "",
        "n_components": "",
    }

    command_options = {
        const.SlBodyOptConsts.sim_dir.value: sim_dir,
        const.SlBodyOptConsts.component.value: "",
        const.SlBodyOptConsts.sim_name.value: realisation_name,
        const.SlBodyOptConsts.fault_name.value: fault_name,
        const.SlBodyOptConsts.n_procs.value: platform_config[
            const.PLATFORM_CONFIG.IM_CALC_DEFAULT_N_CORES.name
        ],
        const.SlBodyOptConsts.extended.value: "",
        const.SlBodyOptConsts.simple_out.value: "",
        const.SlBodyOptConsts.advanced_IM.value: "",
        "pSA_periods": "",
    }

    # Convert option settings to values
    if write_dir is None:
        write_dir = sim_dir

    # Simple vs adv im settings
    if adv_ims:
        # Common values
        proc_type = const.ProcessType.advanced_IM
        sl_template = "adv_im_calc.sl.template"
        script_prefix = "adv_im_calc"

        body_options["models"] = " ".join(
            params[const.SlBodyOptConsts.advanced_IM.value]["models"]
        )
        command_options[
            const.SlBodyOptConsts.advanced_IM.value
        ] = f"-a {body_options['models']}"

        header_options[const.SlHdrOptConsts.additional.value].append(
            "#SBATCH --nodes=1"
        )
        header_options[const.SlHdrOptConsts.n_tasks.value] = body_options["np"] = 18

        # Time for one station to run in hours
        # This should be a machine property. Or take the largest across all machines used
        time_for_one_station = 12 / 60
        est_run_time = np.ceil(station_count / 40) * 2 * time_for_one_station

    else:
        proc_type = const.ProcessType.IM_calculation
        sl_template = "sim_im_calc.sl.template"
        script_prefix = "sim_im_calc"

        if simple_out:
            command_options[const.SlBodyOptConsts.simple_out.value] = "-s"

        if params["ims"][const.RootParams.extended_period.name]:
            command_options[const.SlBodyOptConsts.extended.value] = "-e"
            period_count = len(
                np.unique(np.append(params["ims"]["pSA_periods"], const.EXT_PERIOD))
            )
        else:
            period_count = len(params["ims"]["pSA_periods"])

        if "pSA_periods" in params["ims"]:
            command_options[
                "pSA_periods"
            ] = f"-p {' '.join(str(p) for p in params['ims']['pSA_periods'])}"

        comps_to_store = params["ims"][const.SlBodyOptConsts.component.value]
        command_options[const.SlBodyOptConsts.component.value] = "-c " + " ".join(
            comps_to_store
        )
        body_options["n_components"] = len(comps_to_store)

        # Get wall clock estimation
        logger.info(
            "Running wall clock estimation for IM sim for realisation {}".format(
                realisation_name
            )
        )
        _, est_run_time = est_IM_chours_single(
            station_count,
            int(float(params["sim_duration"]) / float(params["bb"]["dt"])),
            comps_to_store,
            period_count,
            body_options["np"],
            est_model,
        )

    # Header options requiring upstream settings
    header_options["wallclock_limit"] = set_wct(est_run_time, body_options["np"], True)
    header_options["job_name"] = "{}_{}".format(proc_type.str_value, fault_name)
    header_options["platform_specific_args"] = get_platform_node_requirements(1)

    script_file_path = write_sl_script(
        write_dir,
        sim_dir,
        proc_type,
        script_prefix,
        header_options,
        (sl_template, body_options),
        command_options,
    )

    submit_script_to_scheduler(
        script_file_path,
        proc_type.value,
        sim_struct.get_mgmt_db_queue(params["mgmt_db_location"]),
        sim_dir,
        realisation_name,
        target_machine=target_machine,
        logger=logger,
    )
