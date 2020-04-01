#!/usr/bin/env python3
"""Script for automatic submission of gm simulation jobs"""
import argparse
import time
import os

from datetime import datetime
from logging import Logger
from subprocess import call
from typing import List, Dict, Tuple

import numpy as np

from qcore import utils, qclogging
import qcore.constants as const
import qcore.simulation_structure as sim_struct

import estimation.estimate_wct as est
from metadata.log_metadata import store_metadata
from scripts.management.MgmtDB import MgmtDB
from scripts.submit_emod3d import main as submit_lf_main
from scripts.submit_empirical import generate_sl
from scripts.submit_post_emod3d import main as submit_post_lf_main
from scripts.submit_hf import main as submit_hf_main
from scripts.submit_bb import main as submit_bb_main
from scripts.submit_sim_imcalc import submit_im_calc_slurm, SlBodyOptConsts
from shared_workflow import shared_automated_workflow
import shared_workflow.load_config as ldcfg

DEFAULT_N_RUNS = {const.HPC.maui: 12, const.HPC.mahuika: 12}

JOB_RUN_MACHINE = {
    const.ProcessType.EMOD3D: const.HPC.maui,
    const.ProcessType.merge_ts: const.HPC.mahuika,
    const.ProcessType.plot_ts: const.HPC.mahuika,
    const.ProcessType.HF: const.HPC.maui,
    const.ProcessType.BB: const.HPC.maui,
    const.ProcessType.IM_calculation: const.HPC.maui,
    const.ProcessType.IM_plot: const.HPC.mahuika,
    const.ProcessType.rrup: const.HPC.mahuika,
    const.ProcessType.Empirical: const.HPC.mahuika,
    const.ProcessType.Verification: const.HPC.mahuika,
    const.ProcessType.clean_up: const.HPC.mahuika,
    const.ProcessType.LF2BB: const.HPC.mahuika,
    const.ProcessType.HF2BB: const.HPC.mahuika,
    const.ProcessType.plot_srf: const.HPC.mahuika,
    const.ProcessType.advanced_IM: const.HPC.mahuika,
}


AUTO_SUBMIT_LOG_FILE_NAME = "auto_submit_log_{}.txt"


def submit_task(
    sim_dir,
    proc_type,
    run_name,
    root_folder,
    parent_logger,
    retries=None,
    hf_seed=const.HF_DEFAULT_SEED,
    extended_period=False,
    models=None,
):
    task_logger = qclogging.get_task_logger(parent_logger, run_name, proc_type)
    verification_dir = sim_struct.get_verification_dir(sim_dir)
    # Metadata logging setup
    ch_log_dir = os.path.abspath(os.path.join(sim_dir, "ch_log"))
    if not os.path.isdir(ch_log_dir):
        os.mkdir(ch_log_dir)

    submitted_time = datetime.now().strftime(const.METADATA_TIMESTAMP_FMT)
    log_file = os.path.join(sim_dir, "ch_log", const.METADATA_LOG_FILENAME)

    def submit_sl_script(script_name, **kwargs):
        shared_automated_workflow.submit_sl_script(
            script_name,
            proc_type,
            sim_struct.get_mgmt_db_queue(root_folder),
            run_name,
            submit_yes=True,
            logger=task_logger,
            **kwargs,
        )

    if proc_type == const.ProcessType.EMOD3D.value:
        # These have to include the default values (same for all other process types)!
        args = argparse.Namespace(
            auto=True,
            srf=run_name,
            ncore=const.LF_DEFAULT_NCORES,
            account=const.DEFAULT_ACCOUNT,
            machine=JOB_RUN_MACHINE[const.ProcessType.EMOD3D].value,
            rel_dir=sim_dir,
            write_directory=sim_dir,
            retries=retries,
        )
        task_logger.debug("Submit EMOD3D arguments: {}".format(args))
        submit_lf_main(args, est_model=models[0], logger=task_logger)
        store_metadata(
            log_file,
            const.ProcessType.EMOD3D.str_value,
            {"submit_time": submitted_time},
            logger=task_logger,
        )
    elif proc_type == const.ProcessType.merge_ts.value:
        args = argparse.Namespace(
            auto=True,
            srf=run_name,
            account=const.DEFAULT_ACCOUNT,
            machine=JOB_RUN_MACHINE[const.ProcessType.merge_ts].value,
            rel_dir=sim_dir,
            write_directory=sim_dir,
        )
        task_logger.debug("Submit post EMOD3D (merge_ts) arguments: {}".format(args))
        submit_post_lf_main(args, task_logger)
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
        plot_ts_template = (
            "--export=CUR_ENV -o {output_file} -e {error_file} {script_location} "
            "{xyts_path} {srf_path} {output_movie_path} {mgmt_db_loc} {run_name}"
        )
        script = plot_ts_template.format(
            xyts_path=os.path.join(
                sim_struct.get_lf_outbin_dir(sim_dir),
                "{}_xyts.e3d".format(run_name.split("_")[0]),
            ),
            srf_path=sim_struct.get_srf_path(root_folder, run_name),
            output_movie_path=os.path.join(verification_dir, run_name),
            mgmt_db_loc=root_folder,
            run_name=run_name,
            script_location=os.path.expandvars("$gmsim/workflow/scripts/plot_ts.sl"),
            output_file=os.path.join(sim_dir, "%x_%j.out"),
            error_file=os.path.join(sim_dir, "%x_%j.err"),
        )
        submit_sl_script(
            script, target_machine=JOB_RUN_MACHINE[const.ProcessType.plot_ts].value
        )

    elif proc_type == const.ProcessType.HF.value:
        args = argparse.Namespace(
            auto=True,
            srf=run_name,
            seed=hf_seed,
            ncore=const.HF_DEFAULT_NCORES,
            version=const.HF_DEFAULT_VERSION,
            site_specific=None,
            account=const.DEFAULT_ACCOUNT,
            machine=JOB_RUN_MACHINE[const.ProcessType.HF].value,
            rel_dir=sim_dir,
            write_directory=sim_dir,
            debug=False,
            retries=retries,
        )
        task_logger.debug("Submit HF arguments: {}".format(args))
        submit_hf_main(args, models[1], task_logger)
        store_metadata(
            log_file,
            const.ProcessType.HF.str_value,
            {"submit_time": submitted_time},
            logger=task_logger,
        )
    elif proc_type == const.ProcessType.BB.value:
        args = argparse.Namespace(
            auto=True,
            srf=run_name,
            version=const.BB_DEFAULT_VERSION,
            account=const.DEFAULT_ACCOUNT,
            machine=JOB_RUN_MACHINE[const.ProcessType.BB].value,
            rel_dir=sim_dir,
            write_directory=sim_dir,
            retries=retries,
        )
        task_logger.debug("Submit BB arguments: {}".format(args))
        submit_bb_main(args, models[2], task_logger)
        store_metadata(
            log_file,
            const.ProcessType.BB.str_value,
            {"submit_time": submitted_time},
            logger=task_logger,
        )
    elif proc_type == const.ProcessType.IM_calculation.value:
        options_dict = {
            SlBodyOptConsts.extended.value: True if extended_period else False,
            SlBodyOptConsts.simple_out.value: True,
            "auto": True,
            "machine": JOB_RUN_MACHINE[const.ProcessType.IM_calculation].value,
            "write_directory": sim_dir,
        }
        submit_im_calc_slurm(
            sim_dir=sim_dir,
            options_dict=options_dict,
            est_model=models[3],
            logger=task_logger,
        )
        task_logger.debug("Submit IM calc arguments: {}".format(options_dict))
        store_metadata(
            log_file,
            const.ProcessType.IM_calculation.str_value,
            {"submit_time": submitted_time},
            logger=task_logger,
        )
    elif proc_type == const.ProcessType.IM_plot.value:
        im_plot_template = (
            "--export=CUR_ENV -o {output_file} -e {error_file} {script_location} "
            "{csv_path} {station_file_path} {output_xyz_dir} {srf_path} {model_params_path} {mgmt_db_loc} {run_name}"
        )
        params = utils.load_sim_params(os.path.join(sim_dir, "sim_params.yaml"))
        script = im_plot_template.format(
            csv_path=os.path.join(sim_struct.get_IM_csv(sim_dir)),
            station_file_path=params.stat_file,
            output_xyz_dir=os.path.join(verification_dir, "IM_plot"),
            srf_path=sim_struct.get_srf_path(root_folder, run_name),
            model_params_path=params.MODEL_PARAMS,
            mgmt_db_loc=root_folder,
            run_name=run_name,
            script_location=os.path.expandvars("$gmsim/workflow/scripts/im_plot.sl"),
            output_file=os.path.join(sim_dir, "%x_%j.out"),
            error_file=os.path.join(sim_dir, "%x_%j.err"),
        )
        submit_sl_script(
            script, target_machine=JOB_RUN_MACHINE[const.ProcessType.IM_plot].value
        )
    elif proc_type == const.ProcessType.rrup.value:
        submit_sl_script(
            "--output {} --error {} {} {} {}".format(
                os.path.join(sim_dir, "%x_%j.out"),
                os.path.join(sim_dir, "%x_%j.err"),
                os.path.expandvars("$gmsim/workflow/scripts/calc_rrups_single.sl"),
                sim_dir,
                root_folder,
            ),
            target_machine=JOB_RUN_MACHINE[const.ProcessType.rrup].value,
        )
    elif proc_type == const.ProcessType.Empirical.value:
        extended_period_switch = "-e" if extended_period else ""
        sl_script = generate_sl(
            40, extended_period_switch, root_folder, "nesi00213", [run_name], sim_dir
        )
        submit_sl_script(
            sl_script, target_machine=JOB_RUN_MACHINE[const.ProcessType.Empirical].value
        )
    elif proc_type == const.ProcessType.Verification.value:
        pass
    elif proc_type == const.ProcessType.clean_up.value:
        clean_up_template = (
            "--export=CUR_ENV -o {output_file} -e {error_file} {script_location} "
            "{sim_dir} {srf_name} {mgmt_db_loc} "
        )
        script = clean_up_template.format(
            sim_dir=sim_dir,
            srf_name=run_name,
            mgmt_db_loc=root_folder,
            script_location=os.path.expandvars("$gmsim/workflow/scripts/clean_up.sl"),
            output_file=os.path.join(sim_dir, "%x_%j.out"),
            error_file=os.path.join(sim_dir, "%x_%j.err"),
        )
        submit_sl_script(
            script, target_machine=JOB_RUN_MACHINE[const.ProcessType.clean_up].value
        )
    elif proc_type == const.ProcessType.LF2BB.value:
        params = utils.load_sim_params(os.path.join(sim_dir, "sim_params.yaml"))
        submit_sl_script(
            "--output {} --error {} {} {} {} {} {}".format(
                os.path.join(sim_dir, "%x_%j.out"),
                os.path.join(sim_dir, "%x_%j.err"),
                os.path.expandvars("$gmsim/workflow/scripts/lf2bb.sl"),
                sim_dir,
                root_folder,
                utils.load_sim_params(
                    os.path.join(sim_dir, "sim_params.yaml")
                ).stat_vs_est,
                " ".join(
                    ["--{} {}".format(key, item) for key, item in params.bb.items()]
                ),
            ),
            target_machine=JOB_RUN_MACHINE[const.ProcessType.LF2BB].value,
        )
    elif proc_type == const.ProcessType.HF2BB.value:
        params = utils.load_sim_params(os.path.join(sim_dir, "sim_params.yaml"))
        submit_sl_script(
            "--output {} --error {} {} {} {} {}".format(
                os.path.join(sim_dir, "%x_%j.out"),
                os.path.join(sim_dir, "%x_%j.err"),
                os.path.expandvars("$gmsim/workflow/scripts/hf2bb.sl"),
                sim_dir,
                root_folder,
                " ".join(
                    ["--{} {}".format(key, item) for key, item in params.bb.items()]
                ),
            ),
            target_machine=JOB_RUN_MACHINE[const.ProcessType.HF2BB].value,
        )
    elif proc_type == const.ProcessType.plot_srf.value:
        plot_srf_template = (
            "--export=CUR_ENV -o {output_file} -e {error_file} {script_location} "
            "{srf_dir} {output_dir} {mgmt_db_loc} {run_name}"
        )
        script = plot_srf_template.format(
            srf_dir=sim_struct.get_srf_dir(root_folder, run_name),
            output_dir=sim_struct.get_sources_plot_dir(root_folder, run_name),
            mgmt_db_loc=root_folder,
            run_name=run_name,
            script_location=os.path.expandvars("$gmsim/workflow/scripts/plot_srf.sl"),
            output_file=os.path.join(sim_dir, "%x_%j.out"),
            error_file=os.path.join(sim_dir, "%x_%j.err"),
        )
        submit_sl_script(
            script, target_machine=JOB_RUN_MACHINE[const.ProcessType.plot_srf].value
        )
    elif proc_type == const.ProcessType.advanced_IM.value:
        params = utils.load_sim_params(
            os.path.join(sim_dir, "sim_params.yaml"), load_vm=False
        )
        options_dict = {
            "auto": True,
            "machine": JOB_RUN_MACHINE[const.ProcessType.advanced_IM].value,
            "write_directory": sim_dir,
            const.ProcessType.advanced_IM.str_value: params[
                const.ProcessType.advanced_IM.str_value
            ].models,
        }

        submit_im_calc_slurm(
            sim_dir=sim_dir,
            options_dict=options_dict,
            est_model=models[3],
            logger=task_logger,
        )

        task_logger.debug("Submit Advanced_IM calc arguments: {}".format(options_dict))
        store_metadata(
            log_file,
            const.ProcessType.advanced_IM.str_value,
            {"submit_time": submitted_time},
            logger=task_logger,
        )

    qclogging.clean_up_logger(task_logger)


def run_main_submit_loop(
    root_folder: str,
    user: str,
    n_runs: Dict[str, int],
    rels_to_run: str,
    given_tasks_to_run: List[const.ProcessType],
    sleep_time: int,
    models_tuple: Tuple[est.EstModel],
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
    hf_seed, extended_period = const.HF_DEFAULT_SEED, False

    if const.RootParams.seed.value in config["hf"]:
        hf_seed = config["hf"][const.RootParams.seed.value]
    main_logger.debug("hf_seed set to {}".format(hf_seed))

    if "extended_period" in config:
        extended_period = config["extended_period"]
    main_logger.debug("extended_period set to {}".format(extended_period))

    time_since_something_happened = cycle_timeout

    while time_since_something_happened > 0:
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
        for hpc in const.HPC:
            try:
                squeued_tasks = shared_automated_workflow.get_queued_tasks(
                    user=user, machine=hpc
                )
            except EnvironmentError as e:
                main_logger.critical(e)
                n_tasks_to_run[hpc] = 0
            else:
                squeued_tasks.pop(0)
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
        tasks_to_run, task_counter = [], {key: 0 for key in const.HPC}
        for cur_proc_type, cur_run_name, retries in runnable_tasks:

            cur_hpc = JOB_RUN_MACHINE[const.ProcessType(cur_proc_type)]
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
                    [
                        const.ProcessType.clean_up.value,
                        run_name,
                        const.Status.completed.str_value,
                    ]
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
                extended_period=extended_period,
                models=models_tuple,
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
            (x.str_value for x in const.HPC)
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

    args = parser.parse_args()

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
            n_runs = {hpc: args.n_runs[0] for hpc in const.HPC}
            logger.debug(
                "Using {} as the maximum number of jobs per machine".format(
                    args.n_runs[0]
                )
            )
        elif len(args.n_runs) == len(const.HPC):
            n_runs = {}
            for index, hpc in enumerate(const.HPC):
                logger.debug(
                    "Setting {} to have at most {} concurrently running jobs".format(
                        hpc, args.n_runs[index]
                    )
                )
                n_runs.update({hpc: args.n_runs[index]})
        else:
            logger.critical(
                "Expected either 1 or {} values for --n_runs, got {} values. Specifically: {}. Exiting now".format(
                    len(const.HPC), len(args.n_runs), args.n_runs
                )
            )
            parser.error(
                "You must specify wither one common value for --n_runs, or one "
                "for each in the following list: {}".format(list(const.HPC))
            )
    else:
        n_runs = DEFAULT_N_RUNS

    logger.debug(
        "Processes to be run were: {}. Getting all required dependencies now.".format(
            args.task_types_to_run
        )
    )
    task_types_to_run = [
        const.ProcessType.from_str(proc) for proc in args.task_types_to_run
    ]
    for task in task_types_to_run:
        logger.debug(
            "Process {} in processes to be run, adding dependencies now.".format(
                task.str_value
            )
        )
        for proc_num in task.get_remaining_dependencies(task_types_to_run):
            proc = const.ProcessType(proc_num)
            if proc not in task_types_to_run:
                logger.debug(
                    "Process {} added as a dependency of process {}".format(
                        proc.str_value, task.str_value
                    )
                )
                task_types_to_run.append(proc)

    mutually_exclusive_task_error = const.ProcessType.check_mutually_exclusive_tasks(
        task_types_to_run
    )
    if mutually_exclusive_task_error != "":
        logger.log(qclogging.NOPRINTCRITICAL, mutually_exclusive_task_error)
        parser.error(mutually_exclusive_task_error)

    logger.debug("Processed args are as follows: {}".format(str(args)))

    logger.info("Loading estimation models")
    workflow_config = ldcfg.load()
    lf_est_model = est.load_full_model(
        os.path.join(workflow_config["estimation_models_dir"], "LF"), logger=logger
    )
    hf_est_model = est.load_full_model(
        os.path.join(workflow_config["estimation_models_dir"], "HF"), logger=logger
    )
    bb_est_model = est.load_full_model(
        os.path.join(workflow_config["estimation_models_dir"], "BB"), logger=logger
    )
    im_est_model = est.load_full_model(
        os.path.join(workflow_config["estimation_models_dir"], "IM"), logger=logger
    )

    run_main_submit_loop(
        root_folder,
        args.user,
        n_runs,
        args.rels_to_run,
        task_types_to_run,
        args.sleep_time,
        (lf_est_model, hf_est_model, bb_est_model, im_est_model),
        main_logger=logger,
    )


if __name__ == "__main__":
    main()
