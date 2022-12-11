#!/usr/bin/env python3
"""Script for automatic submission of gm simulation jobs"""
from collections import OrderedDict
from pathlib import Path
import argparse
import os
import time

from datetime import datetime
from logging import Logger
from typing import List, Dict
import numpy as np

from qcore import utils, qclogging
import qcore.constants as const
import qcore.simulation_structure as sim_struct


from workflow.automation.metadata.log_metadata import store_metadata
from workflow.automation.lib.MgmtDB import MgmtDB, ComparisonOperator
from workflow.automation.lib.schedulers.scheduler_factory import Scheduler
from workflow.automation.submit.submit_emod3d import main as submit_lf_main
from workflow.automation.submit.submit_empirical import generate_empirical_script
from workflow.automation.submit.submit_post_emod3d import main as submit_post_lf_main
from workflow.automation.submit.submit_hf import main as submit_hf_main
from workflow.automation.submit.submit_bb import main as submit_bb_main
from workflow.automation.submit.submit_sim_imcalc import submit_im_calc_slurm
from workflow.automation.submit.submit_vm_pert import submit_vm_pert_main
from workflow.automation.lib import shared_automated_workflow
from workflow.automation.platform_config import (
    HPC,
    platform_config,
    get_platform_specific_script,
    get_target_machine,
)

AUTO_SUBMIT_LOG_FILE_NAME = "auto_submit_log_{}.txt"


def submit_task(
    sim_dir,
    proc_type,
    run_name,
    root_folder,
    parent_logger,
    retries=None,
    hf_seed=const.HF_DEFAULT_SEED,
):
    task_logger = qclogging.get_task_logger(parent_logger, run_name, proc_type)
    verification_dir = sim_struct.get_verification_dir(sim_dir)
    # Metadata logging setup
    ch_log_dir = os.path.abspath(os.path.join(sim_dir, "ch_log"))
    if not os.path.isdir(ch_log_dir):
        os.mkdir(ch_log_dir)

    load_vm_params = const.ProcessType(proc_type) is not const.ProcessType.VM_PARAMS

    params = utils.load_sim_params(
        sim_struct.get_sim_params_yaml_path(sim_dir), load_vm=load_vm_params
    )

    submitted_time = datetime.now().strftime(const.METADATA_TIMESTAMP_FMT)
    log_file = os.path.join(sim_dir, "ch_log", const.METADATA_LOG_FILENAME)

    def submit_script_to_scheduler(script_name, target_machine=None, **kwargs):
        shared_automated_workflow.submit_script_to_scheduler(
            script_name,
            proc_type,
            sim_struct.get_mgmt_db_queue(root_folder),
            sim_dir,
            run_name,
            target_machine=target_machine,
            logger=task_logger,
        )

    if proc_type == const.ProcessType.EMOD3D.value:
        # These have to include the default values (same for all other process types)!
        task_logger.debug("Submit EMOD3D arguments: {}".format(run_name))
        submit_lf_main(
            submit=True,
            machine=get_target_machine(const.ProcessType.EMOD3D).name,
            ncores=platform_config[const.PLATFORM_CONFIG.LF_DEFAULT_NCORES.name],
            rel_dir=sim_dir,
            retries=retries,
            write_directory=sim_dir,
            logger=task_logger,
        )
        store_metadata(
            log_file,
            const.ProcessType.EMOD3D.str_value,
            {"submit_time": submitted_time},
            logger=task_logger,
        )
    elif proc_type == const.ProcessType.merge_ts.value:
        task_logger.debug(
            "Submit post EMOD3D (merge_ts) arguments: {}".format(run_name)
        )
        submit_post_lf_main(
            submit=True,
            machine=get_target_machine(const.ProcessType.merge_ts).name,
            rel_dir=sim_dir,
            write_directory=sim_dir,
            logger=task_logger,
        )
        store_metadata(
            log_file,
            const.ProcessType.merge_ts.str_value,
            {"submit_time": submitted_time},
            logger=task_logger,
        )
    elif proc_type == const.ProcessType.plot_ts.value:
        # plot_ts.py does not mkdir dir if output dir does not exist,
        # whereas im_plot does.
        if not os.path.isdir(verification_dir):
            os.mkdir(verification_dir)
        arguments = OrderedDict(
            {
                "XYTS_PATH": os.path.join(
                    sim_struct.get_lf_outbin_dir(sim_dir),
                    "{}_xyts.e3d".format(run_name),
                ),
                "SRF_PATH": sim_struct.get_srf_path(root_folder, run_name),
                "OUTPUT_TS_PATH": os.path.join(verification_dir, run_name),
                "MGMT_DB_LOC": root_folder,
                "SRF_NAME": run_name,
            }
        )

        script = get_platform_specific_script(const.ProcessType.plot_ts, arguments)

        submit_script_to_scheduler(
            script, target_machine=get_target_machine(const.ProcessType.plot_ts).name
        )

    elif proc_type == const.ProcessType.HF.value:
        task_logger.debug("Submit HF arguments: {}".format(run_name))
        submit_hf_main(
            submit=True,
            machine=get_target_machine(const.ProcessType.HF).name,
            ncores=platform_config[const.PLATFORM_CONFIG.HF_DEFAULT_NCORES.name],
            rel_dir=sim_dir,
            retries=retries,
            seed=hf_seed,
            # site_specific=None,
            version=platform_config[const.PLATFORM_CONFIG.HF_DEFAULT_VERSION.name],
            write_directory=sim_dir,
            logger=task_logger,
        )
        store_metadata(
            log_file,
            const.ProcessType.HF.str_value,
            {"submit_time": submitted_time},
            logger=task_logger,
        )
    elif proc_type == const.ProcessType.BB.value:
        task_logger.debug("Submit BB arguments: {}".format(run_name))
        submit_bb_main(
            submit=True,
            machine=get_target_machine(const.ProcessType.BB).name,
            rel_dir=sim_dir,
            retries=retries,
            version=platform_config[const.PLATFORM_CONFIG.BB_DEFAULT_VERSION.name],
            write_directory=sim_dir,
            logger=task_logger,
        )
        store_metadata(
            log_file,
            const.ProcessType.BB.str_value,
            {"submit_time": submitted_time},
            logger=task_logger,
        )
    elif proc_type == const.ProcessType.IM_calculation.value:
        submit_im_calc_slurm(
            sim_dir=sim_dir,
            simple_out=True,
            retries=retries,
            target_machine=get_target_machine(const.ProcessType.IM_calculation).name,
            logger=task_logger,
        )
        task_logger.debug(
            f"Submit IM calc arguments: sim_dir: {sim_dir}, simple_out: True, target_machine: {get_target_machine(const.ProcessType.IM_calculation).name}"
        )
        store_metadata(
            log_file,
            const.ProcessType.IM_calculation.str_value,
            {"submit_time": submitted_time},
            logger=task_logger,
        )
    elif proc_type == const.ProcessType.IM_plot.value:
        arguments = OrderedDict(
            {
                "CSV_PATH": sim_struct.get_IM_csv(sim_dir),
                "STATION_FILE_PATH": params.stat_file,
                "OUTPUT_XYZ_PARENT_DIR": os.path.join(verification_dir, "IM_plot"),
                "SRF_PATH": sim_struct.get_srf_path(root_folder, run_name),
                "MODEL_PARAMS": os.path.join(
                    sim_struct.get_fault_VM_dir(root_folder, run_name),
                    os.path.basename(params.MODEL_PARAMS),
                ),
                "MGMT_DB_LOC": root_folder,
                "SRF_NAME": run_name,
            }
        )
        script = get_platform_specific_script(const.ProcessType.IM_plot, arguments)
        submit_script_to_scheduler(
            script, target_machine=get_target_machine(const.ProcessType.IM_plot).name
        )
    elif proc_type == const.ProcessType.rrup.value:
        submit_script_to_scheduler(
            get_platform_specific_script(
                const.ProcessType.rrup,
                OrderedDict({"REL": sim_dir, "MGMT_DB_LOC": root_folder}),
            ),
            target_machine=get_target_machine(const.ProcessType.rrup).name,
        )
    elif proc_type == const.ProcessType.Empirical.value:
        extended_period_switch = "-e" if params["ims"]["extended_period"] else ""
        sl_script = generate_empirical_script(
            1, extended_period_switch, root_folder, [run_name], sim_dir
        )
        submit_script_to_scheduler(
            sl_script,
            target_machine=get_target_machine(const.ProcessType.Empirical).name,
        )
    elif proc_type == const.ProcessType.Verification.value:
        raise NotImplementedError("Verification is not currently working")
    elif proc_type == const.ProcessType.clean_up.value:
        submit_script_to_scheduler(
            get_platform_specific_script(
                const.ProcessType.clean_up,
                OrderedDict(
                    {
                        "SIM_DIR": sim_dir,
                        "SRF_NAME": run_name,
                        "MGMT_DB_LOC": root_folder,
                    }
                ),
            ),
            target_machine=get_target_machine(const.ProcessType.clean_up).name,
        )
    elif proc_type == const.ProcessType.LF2BB.value:
        submit_script_to_scheduler(
            get_platform_specific_script(
                const.ProcessType.LF2BB,
                OrderedDict(
                    {
                        "REL_LOC": sim_dir,
                        "MGMT_DB_LOC": root_folder,
                        "VSITE_FILE": params.stat_vs_est,
                        "REM_ARGS": "'"
                        + " ".join(
                            [
                                "--{} {}".format(key, item)
                                for key, item in params.bb.items()
                            ]
                        )
                        + "'",
                    }
                ),
            ),
            target_machine=get_target_machine(const.ProcessType.LF2BB).name,
        )
    elif proc_type == const.ProcessType.HF2BB.value:
        submit_script_to_scheduler(
            get_platform_specific_script(
                const.ProcessType.HF2BB,
                OrderedDict(
                    {
                        "REL_LOC": sim_dir,
                        "MGMT_DB_LOC": root_folder,
                        "REM_ARGS": "'"
                        + " ".join(
                            [
                                "--{} {}".format(key, item)
                                for key, item in params.bb.items()
                            ]
                        )
                        + "'",
                    }
                ),
            ),
            target_machine=get_target_machine(const.ProcessType.HF2BB).name,
        )
    elif proc_type == const.ProcessType.plot_srf.value:
        submit_script_to_scheduler(
            get_platform_specific_script(
                const.ProcessType.plot_srf,
                OrderedDict(
                    {
                        "SRF_DIR": sim_struct.get_srf_dir(root_folder, run_name),
                        "OUTPUT_DIR": sim_struct.get_sources_plot_dir(
                            root_folder, run_name
                        ),
                        "MGMT_DB_LOC": root_folder,
                        "SRF_NAME": run_name,
                    }
                ),
            ),
            target_machine=get_target_machine(const.ProcessType.plot_srf).name,
        )
    elif proc_type == const.ProcessType.advanced_IM.value:
        submit_im_calc_slurm(
            sim_dir=sim_dir,
            adv_ims=True,
            target_machine=get_target_machine(const.ProcessType.IM_calculation).name,
            logger=task_logger,
        )

        task_logger.debug(
            f"Submit Advanced_IM calc arguments:sim_dir: {sim_dir}, adv_im: True, target_machine: {get_target_machine(const.ProcessType.IM_calculation).name}"
        )
        store_metadata(
            log_file,
            const.ProcessType.advanced_IM.str_value,
            {"submit_time": submitted_time},
            logger=task_logger,
        )
    elif proc_type == const.ProcessType.VM_PARAMS.value:
        submit_script_to_scheduler(
            get_platform_specific_script(
                const.ProcessType.VM_PARAMS,
                OrderedDict(
                    {
                        "realisationCSV": str(
                            Path(sim_struct.get_srf_dir(root_folder, run_name))
                            / (run_name + ".csv")
                        ),
                        "OUTPUT_DIR": sim_struct.get_fault_VM_dir(
                            root_folder, run_name
                        ),
                        "VM_VERSION": str(params["VM"]["VM_Version"]),
                        "VM_TOPO": str(params["VM"]["VM_Topo"]),
                        "HH": str(params["VM"]["hh"]),
                        "PGV_THRESHOLD": str(params["VM"]["PGV_Threshold"]),
                        "DS_MULTIPLIER": str(params["VM"]["Ds_Multiplier"]),
                        "MGMT_DB_LOC": root_folder,
                        "REL_NAME": run_name,
                    }
                ),
            ),
            target_machine=get_target_machine(const.ProcessType.VM_PARAMS).name,
        )
    elif proc_type == const.ProcessType.VM_GEN.value:
        submit_script_to_scheduler(
            get_platform_specific_script(
                const.ProcessType.VM_GEN,
                OrderedDict(
                    {
                        "VM_PARAMS_YAML": str(
                            Path(sim_struct.get_fault_VM_dir(root_folder, run_name))
                            / "vm_params.yaml"
                        ),
                        "OUTPUT_DIR": sim_struct.get_fault_VM_dir(
                            root_folder, run_name
                        ),
                        "SRF_PATH": sim_struct.get_srf_path(root_folder, run_name),
                        "MGMT_DB_LOC": root_folder,
                        "REL_NAME": run_name,
                    }
                ),
            ),
            target_machine=get_target_machine(const.ProcessType.VM_GEN).name,
        )
    elif proc_type == const.ProcessType.VM_PERT.value:
        submit_vm_pert_main(root_folder, run_name, sim_dir, logger=task_logger)
    elif proc_type == const.ProcessType.INSTALL_FAULT.value:
        fault_dir = sim_struct.get_fault_dir(
            root_folder, sim_struct.get_fault_from_realisation(run_name)
        )
        submit_script_to_scheduler(
            get_platform_specific_script(
                const.ProcessType.INSTALL_FAULT,
                OrderedDict(
                    {
                        "VM_PARAMS_YAML": str(
                            Path(sim_struct.get_fault_VM_dir(root_folder, run_name))
                            / "vm_params.yaml"
                        ),
                        "STAT_FILE": str(params.stat_file),
                        "FAULT_DIR": fault_dir,
                        "FDSTATLIST": str(Path(fault_dir) / f"fd{str(params.sufx)}.ll"),
                        "MGMT_DB_LOC": root_folder,
                        "REL_NAME": run_name,
                    }
                ),
            ),
            target_machine=get_target_machine(const.ProcessType.INSTALL_FAULT).name,
        )
    elif proc_type == const.ProcessType.SRF_GEN.value:
        submit_script_to_scheduler(
            get_platform_specific_script(
                const.ProcessType.SRF_GEN,
                OrderedDict(
                    {
                        "REL_YAML": str(
                            Path(sim_struct.get_srf_path(root_folder, run_name)).parent
                            / f"{run_name}.yaml"
                        ),
                        "MGMT_DB_LOC": root_folder,
                        "REL_NAME": run_name,
                    }
                ),
            ),
            target_machine=get_target_machine(const.ProcessType.SRF_GEN).name,
        )

    qclogging.clean_up_logger(task_logger)


def run_main_submit_loop(
    root_folder: str,
    n_runs: Dict[str, int],
    rels_to_run: str,
    given_tasks_to_run: List[const.ProcessType],
    sleep_time: int,
    matcher: ComparisonOperator = ComparisonOperator.LIKE,
    main_logger: Logger = qclogging.get_basic_logger(),
    cycle_timeout=1,
):
    mgmt_queue_folder = sim_struct.get_mgmt_db_queue(root_folder)
    mgmt_db = MgmtDB(sim_struct.get_mgmt_db(root_folder))
    root_params_file = os.path.join(
        sim_struct.get_runs_dir(root_folder), "root_params.yaml"
    )
    config = utils.load_yaml(root_params_file)
    main_logger.info("Loaded root params file: {}".format(root_params_file))
    # Default values

    hf_seed = config["hf"].get(const.RootParams.seed.value, const.HF_DEFAULT_SEED)
    main_logger.debug("hf_seed set to {}".format(hf_seed))

    main_logger.debug(f"extended_period set to {config['ims']['extended_period']}")

    time_since_something_happened = cycle_timeout

    first = True
    while time_since_something_happened > 0 or first:
        first = False
        main_logger.debug(
            "time_since_something_happened is now {}".format(
                time_since_something_happened
            )
        )
        time_since_something_happened -= 1
        # Get items in the mgmt queue, have to get a snapshot instead of
        # checking the directory real-time to prevent timing issues,
        # which can result in dual-submission
        mgmt_queue_entries = os.listdir(mgmt_queue_folder)

        # Get in progress tasks in the db and the HPC queue
        n_tasks_to_run = {}
        for hpc in HPC:
            try:
                squeued_tasks = Scheduler.get_scheduler().check_queues(
                    user=True, target_machine=hpc
                )
            except EnvironmentError as e:
                main_logger.critical(e)
                n_tasks_to_run[hpc] = 0
            else:
                n_tasks_to_run[hpc] = n_runs[hpc] - len(squeued_tasks)
                if len(squeued_tasks) > 0:
                    main_logger.debug(
                        "There was at least one job in squeue, resetting timeout"
                    )
                    time_since_something_happened = cycle_timeout

        # Gets all runnable tasks based on mgmt db state
        runnable_tasks = mgmt_db.get_runnable_tasks(
            rels_to_run,
            sum(n_runs.values()),
            os.listdir(sim_struct.get_mgmt_db_queue(root_folder)),
            matcher,
            given_tasks_to_run,
            main_logger,
        )
        if len(runnable_tasks) > 0:
            time_since_something_happened = cycle_timeout
            main_logger.info("Number of runnable tasks: {}".format(len(runnable_tasks)))
            main_logger.debug("There was at least one runnable task, resetting timeout")
        else:
            main_logger.debug("No runnable_tasks")

        # Select the first ntask_to_run that are not waiting
        # for mgmt db updates (i.e. items in the queue)
        tasks_to_run, task_counter = [], {key: 0 for key in HPC}
        for cur_proc_type, cur_run_name, retries in runnable_tasks:

            cur_hpc = get_target_machine(cur_proc_type)
            # Add task if limit has not been reached and there are no
            # outstanding mgmt db updates
            if (
                not shared_automated_workflow.check_mgmt_queue(
                    mgmt_queue_entries, cur_run_name, cur_proc_type
                )
                and task_counter.get(cur_hpc, 0) < n_tasks_to_run[cur_hpc]
            ):
                tasks_to_run.append((cur_proc_type, cur_run_name, retries))
                task_counter[cur_hpc] += 1

            # Open to better suggestions
            # Break if enough tasks for each HPC have been added
            if np.all(
                [
                    True if task_counter.get(hpc, 0) >= n_tasks_to_run[hpc] else False
                    for hpc in n_tasks_to_run.keys()
                ]
            ):
                break

        if len(tasks_to_run) > 0:
            main_logger.info(
                "Tasks to run this iteration: "
                + ", ".join(
                    [
                        "{}-{}".format(entry[1], const.ProcessType(entry[0]).str_value)
                        for entry in tasks_to_run
                    ]
                )
            )
        else:
            main_logger.debug("No tasks to run this iteration")

        # Submit the runnable tasks
        for proc_type, run_name, retries in tasks_to_run:

            # Special handling for merge-ts
            if proc_type == const.ProcessType.merge_ts.value:
                # Check if clean up has already run
                if mgmt_db.is_task_complete(
                    [const.ProcessType.clean_up.value, run_name]
                ):
                    # If clean_up has already run, then we should set it to
                    # be run again after merge_ts has run
                    shared_automated_workflow.add_to_queue(
                        mgmt_queue_folder,
                        run_name,
                        const.ProcessType.clean_up.value,
                        const.Status.created.value,
                        logger=main_logger,
                    )

            # submit the job
            submit_task(
                sim_struct.get_sim_dir(root_folder, run_name),
                proc_type,
                run_name,
                root_folder,
                main_logger,
                retries=retries,
                hf_seed=hf_seed,
            )
        main_logger.debug("Sleeping for {} second(s)".format(sleep_time))
        time.sleep(sleep_time)
    main_logger.info("Nothing was running or ready to run last cycle, exiting now")


def main():
    logger = qclogging.get_logger()

    parser = argparse.ArgumentParser()

    parser.add_argument("root_folder", type=str, help="The cybershake root folder")
    parser.add_argument(
        "--n_runs",
        default=None,
        type=int,
        nargs="+",
        help="The number of processes each machine can run at once. If a single value is given this is used for all "
        "machines, otherwise one value per machine must be given. The current order is: {}".format(
            (x.name for x in HPC)
        ),
    )
    parser.add_argument(
        "user", type=str, help="The username under which the jobs will be submitted."
    )
    parser.add_argument(
        "--sleep_time",
        type=int,
        help="Seconds sleeping between checking queue and adding more jobs",
        default=5,
    )
    parser.add_argument(
        "--log_file",
        type=str,
        default=None,
        help="Location of the log file to use. Defaults to 'cybershake_log.txt' in the location root_folder. "
        "Must be absolute or relative to the root_folder.",
    )
    parser.add_argument(
        "--task_types_to_run",
        nargs="+",
        help="Which processes should be run. Defaults to IM_Calc and clean_up with dependencies automatically propagated",
        choices=[proc.str_value for proc in const.ProcessType],
        default=[const.ProcessType.clean_up.str_value],
    )
    parser.add_argument(
        "--rels_to_run",
        help="An SQLite formatted query to match the realisations that should run.",
        default="%",
    )
    parser.add_argument(
        "--matcher",
        help="Type of SQL match to make. Either: "
        "EXACT for exact only match, "
        "LIKE to match any '%' or '_' symbols, or "
        "NOTLIKE to match everything except the --rels_to_run argument",
        default=ComparisonOperator.LIKE.name,
        options=ComparisonOperator.get_names(),
    )

    args = parser.parse_args()
    args.matcher = ComparisonOperator[args.matcher]
    root_folder = os.path.abspath(args.root_folder)

    if args.log_file is None:
        qclogging.add_general_file_handler(
            logger,
            os.path.join(
                root_folder,
                AUTO_SUBMIT_LOG_FILE_NAME.format(
                    datetime.now().strftime(const.TIMESTAMP_FORMAT)
                ),
            ),
        )
    else:
        qclogging.add_general_file_handler(
            logger, os.path.join(root_folder, args.log_file)
        )
    logger.debug("Added file handler to the logger")

    logger.debug("Raw args passed in as follows: {}".format(str(args)))

    n_runs = 0
    if args.n_runs is not None:
        if len(args.n_runs) == 1:
            n_runs = {hpc: args.n_runs[0] for hpc in HPC}
            logger.debug(
                "Using {} as the maximum number of jobs per machine".format(
                    args.n_runs[0]
                )
            )
        elif len(args.n_runs) == len(HPC):
            n_runs = {}
            for index, hpc in enumerate(HPC):
                logger.debug(
                    "Setting {} to have at most {} concurrently running jobs".format(
                        hpc, args.n_runs[index]
                    )
                )
                n_runs.update({hpc: args.n_runs[index]})
        else:
            logger.critical(
                "Expected either 1 or {} values for --n_runs, got {} values. Specifically: {}. Exiting now".format(
                    len(HPC), len(args.n_runs), args.n_runs
                )
            )
            parser.error(
                "You must specify wither one common value for --n_runs, or one "
                "for each in the following list: {}".format(list(HPC))
            )
    else:
        n_runs = platform_config[const.PLATFORM_CONFIG.DEFAULT_N_RUNS.name]

    logger.debug(
        "Processes to be run were: {}. "
        "Getting all required dependencies now. "
        "Assuming all given tasks are to be run for all realisations (including median, as per --rels_to_run)".format(
            args.task_types_to_run
        )
    )
    task_types_to_run = [const.Dependency(proc) for proc in args.task_types_to_run]
    for dependency in task_types_to_run:
        logger.debug(
            f"Process {dependency} in processes to be run, adding dependencies now."
        )
        for sub_dependency in dependency.process.get_remaining_dependencies(
            task_types_to_run
        ):
            target = sub_dependency
            if target not in task_types_to_run:
                logger.debug(
                    f"Dependency {target.process} added as a dependency of process {dependency.process}"
                )
                task_types_to_run.append(target)
    task_types_to_run = [x.process for x in task_types_to_run]

    mutually_exclusive_task_error = const.ProcessType.check_mutually_exclusive_tasks(
        task_types_to_run
    )
    if mutually_exclusive_task_error != "":
        logger.log(qclogging.NOPRINTCRITICAL, mutually_exclusive_task_error)
        parser.error(mutually_exclusive_task_error)

    logger.debug("Processed args are as follows: {}".format(str(args)))

    scheduler_logger = qclogging.get_logger(name=f"{logger.name}.scheduler")
    Scheduler.initialise_scheduler(user=args.user, logger=scheduler_logger)
    run_main_submit_loop(
        root_folder,
        n_runs,
        args.rels_to_run,
        task_types_to_run,
        args.sleep_time,
        args.matcher,
        main_logger=logger,
    )


if __name__ == "__main__":
    main()
