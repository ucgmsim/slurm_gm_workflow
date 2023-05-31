import glob
from logging import Logger
import numpy as np
from os import path
import sys

from qcore import constants as const
from qcore import utils
from qcore.formats import load_station_file
from qcore.qclogging import get_basic_logger
from qcore import simulation_structure as sim_struct
from qcore.timeseries import get_observed_stations, BBSeis
from qcore.config import qconfig

from workflow.automation.estimation import estimate_wct
from workflow.automation.platform_config import (
    platform_config,
    get_platform_node_requirements,
    get_target_machine,
)
from workflow.automation.lib.shared_automated_workflow import submit_script_to_scheduler
from workflow.automation.lib.shared_template import write_sl_script


def submit_im_calc_slurm(
    sim_dir: str,
    write_dir: str = None,
    simple_out: bool = True,
    adv_ims: bool = False,
    retries: int = 0,
    target_machine: str = get_target_machine(const.ProcessType.IM_calculation).name,
    logger: Logger = get_basic_logger(),
):
    """Creates the IM calc slurm scrip, also submits if specified

    The options_dict is populated by the DEFAULT_OPTIONS, values can be changed by
    passing in a dict containing the entries that require changing. Merges the
    two dictionaries, the passed in one has higher priority.
    """
    # Load the yaml params
    params = utils.load_sim_params(
        sim_struct.get_sim_params_yaml_path(sim_dir), load_vm=True
    )
    realisation_name = params[const.SimParams.run_name.value]
    fault_name = sim_struct.get_fault_from_realisation(realisation_name)
    station_count = len(load_station_file(params["FD_STATLIST"]).index)

    header_options = {
        const.SlHdrOptConsts.description.value: "Calculates intensity measures.",
        const.SlHdrOptConsts.memory.value: "2G",
        const.SlHdrOptConsts.version.value: "slurm",
        "exe_time": const.timestamp,
        const.SlHdrOptConsts.additional.value: "#SBATCH --hint=nomultithread"
        if platform_config[const.PLATFORM_CONFIG.SCHEDULER.name] == "slurm"
        else [""],
    }

    body_options = {
        const.SlBodyOptConsts.component.value: "",
        "realisation_name": realisation_name,
        const.SlBodyOptConsts.fault_name.value: fault_name,
        "np": platform_config[const.PLATFORM_CONFIG.IM_CALC_DEFAULT_N_CORES.name],
        "sim_IM_calc_dir": sim_struct.get_im_calc_dir(sim_dir),
        "output_csv": sim_struct.get_IM_csv(sim_dir),
        "output_info": sim_struct.get_IM_info(sim_dir),
        "models": "",
        const.SlBodyOptConsts.mgmt_db.value: "",
        "n_components": "",
        "match_obs_stations": False,
        "station_file": "$(cat $fd_name | awk '{print $3}')",
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
        "run_command": platform_config[const.PLATFORM_CONFIG.RUN_COMMAND.name],
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
        ] = f"-a {body_options['models']} --OpenSees {qconfig['OpenSees']} "

        # create temporary station list if "match_obs_stations" is directory
        if path.isdir(
            str(params[const.SlBodyOptConsts.advanced_IM.value]["match_obs_stations"])
        ):
            logger.debug(
                "match_obs_station specified: {params[const.SlBodyOptConsts.advanced_IM.value]['match_obs_stations']}"
            )
            # retreived station list from observed/fault(eventname)/Vol*/data/accBB/station.
            obs_accBB_dir_glob = path.join(
                params[const.SlBodyOptConsts.advanced_IM.value]["match_obs_stations"],
                f"{fault_name}/*/*/accBB",
            )
            obs_accBB_dir = glob.glob(obs_accBB_dir_glob)
            if len(obs_accBB_dir) > 1:
                logger.error(
                    "got more than one folder globbed. please double check the path to the match_obs_stations is correct."
                )
                sys.exit()
            station_names_observed = set(get_observed_stations(obs_accBB_dir[0]))
            station_names_simulated = set(
                BBSeis(f"{sim_dir}/BB/Acc/BB.bin").stations.name
            )
            station_names_tmp = list(station_names_observed & station_names_simulated)
            # write to a tmp file
            tmp_station_file = path.join(sim_dir, "tmp_station_file")
            with open(tmp_station_file, "w") as f:
                for station in station_names_tmp:
                    f.write(f"{station} ")
            body_options["station_file"] = f"$(cat {tmp_station_file})"
            command_options[const.SlBodyOptConsts.advanced_IM.value] = (
                command_options[const.SlBodyOptConsts.advanced_IM.value]
                + f"--station_names `cat {tmp_station_file}`"
            )

        # Time for one station to run in hours
        # This should be a machine property. Or take the largest across all machines used
        time_for_one_station = 0.5
        est_run_time = (
            np.ceil(station_count / qconfig["cores_per_node"])
            * 2
            * time_for_one_station
        )
        n_cores = body_options["np"]

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
        _, est_run_time, n_cores = estimate_wct.est_IM_chours(
            station_count,
            int(float(params["sim_duration"]) / float(params["dt"])),
            comps_to_store,
            period_count,
            body_options["np"],
        )

    # Header options requiring upstream settings
    # special treatment for im_calc, as the scaling feature in estimation is not suitable
    # cap the wct, otherwise cannot submit
    est_run_time = est_run_time * (int(retries) + 1)
    n_cores, est_run_time = estimate_wct.confine_wct_node_parameters(
        n_cores,
        est_run_time,
        preserve_core_count=retries > 0,
        hyperthreaded=const.ProcessType.IM_calculation.is_hyperth,
        can_checkpoint=True,  # hard coded for now as this is not available programatically
        logger=logger,
    )
    # set ch_safety_factor=1 as we scale it already.
    header_options["wallclock_limit"] = estimate_wct.convert_to_wct(est_run_time)
    logger.debug("Using WCT for IM_calc: {header_options['wallclock_limit']}")
    header_options["job_name"] = "{}_{}".format(proc_type.str_value, fault_name)
    header_options["platform_specific_args"] = get_platform_node_requirements(n_cores)

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
