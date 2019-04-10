#!/usr/bin/env python3
"""Script for automatic submission of gm simulation jobs"""
import argparse
import time
import os
import sys
from datetime import datetime
from subprocess import call, Popen, PIPE
from typing import List

import shlex
import numpy as np

import shared_workflow.load_config as ldcfg
import qcore.constants as const
import qcore.simulation_structure as sim_struct
import estimation.estimate_wct as est
from scripts.management.MgmtDB import MgmtDB, SlurmTask
from metadata.log_metadata import store_metadata

from scripts.submit_emod3d import main as submit_lf_main
from scripts.submit_post_emod3d import main as submit_post_lf_main
from scripts.submit_hf import main as submit_hf_main
from scripts.submit_bb import main as submit_bb_main
from scripts.submit_sim_imcalc import submit_im_calc_slurm, SlBodyOptConsts
from shared_workflow import shared

DEFAULT_N_MAX_RETRIES = 2
DEFAULT_N_RUNS = {const.HPC.maui: 12, const.HPC.mahuika: 12}
DEFAULT_1D_MOD = "/nesi/transit/nesi00213/VelocityModel/Mod-1D/Cant1D_v2-midQ_leer.1d"

JOB_RUN_MACHINE = {
    const.ProcessType.EMOD3D: const.HPC.maui,
    const.ProcessType.merge_ts: const.HPC.mahuika,
    const.ProcessType.winbin_aio: const.HPC.maui,
    const.ProcessType.HF: const.HPC.maui,
    const.ProcessType.BB: const.HPC.maui,
    const.ProcessType.IM_calculation: const.HPC.maui,
    const.ProcessType.clean_up: const.HPC.mahuika,
}

SLURM_TO_STATUS_DICT = {"R": 3, "PD": 2, "CG": 3}


def get_queued_tasks(user=None, machine=const.HPC.maui):
    # TODO: Treat Maui and Mahuika jobs seperately. See QSW-912
    if user is not None:
        cmd = "squeue -A nesi00213 -o '%A %t' -h -M {} -u {}".format(
            machine.value, user
        )
    else:
        cmd = "squeue -A nesi00213 -o '%A %t' -h -M {}".format(machine.value)

    process = Popen(shlex.split(cmd), stdout=PIPE, encoding="utf-8")
    (output, err) = process.communicate()
    process.wait()

    output_list = list(filter(None, output.split("\n")[1:]))
    return output_list


def check_queue(queue_folder: str, run_name: str, proc_type: int):
    """Returns True if there are any queued entries for this run_name and process type,
    otherwise returns False.
    """
    queue_entries = os.listdir(queue_folder)
    for entry in queue_entries:
        _, entry_run_name, entry_proc_type = entry.split(".")
        if entry_run_name == run_name and entry_proc_type == str(proc_type):
            return True
    return False


def update_tasks(queue_folder: str, squeue_tasks, db_tasks: List[SlurmTask]):
    """Updates the mgmt db entries based on the HPC queue"""
    for db_task in db_tasks:
        found = False
        for queue_task in squeue_tasks:
            queue_job_id, queue_status = queue_task.split()
            if db_task.job_id == int(queue_job_id):
                found = True
                try:
                    queue_status = SLURM_TO_STATUS_DICT[queue_status]
                except KeyError:
                    print(
                        "Failed to recogize state code {}, updating to {}".format(
                            queue_status, const.Status.unknown.value
                        )
                    )
                    queue_status = const.Status.unknown.value
                if queue_status == db_task.status:
                    print(
                        "No need to update status {} for {}, {} ({}) as it "
                        "has not changed.".format(
                            const.Status(queue_status).str_value,
                            db_task.run_name,
                            const.ProcessType(db_task.proc_type).str_value,
                            db_task.job_id,
                        )
                    )
                # Do nothing if there is a pending update for
                # this run & process type combination
                elif not check_queue(queue_folder, db_task.run_name, db_task.proc_type):
                    print(
                        "Updating status of {}, {} from {} to {}".format(
                            db_task.run_name,
                            const.ProcessType(db_task.proc_type).str_value,
                            const.Status(db_task.status).str_value,
                            queue_status,
                        )
                    )
                    shared.add_to_queue(
                        queue_folder, db_task.run_name, db_task.proc_type, queue_status
                    )
        # Only reset if there is no entry on the mgmt queue for this
        # realisation/proc combination
        # Ignore cleanup for now as it runs on mahuika
        if (
            not found
            and not check_queue(queue_folder, db_task.run_name, db_task.proc_type)
            and const.ProcessType(db_task.proc_type) != const.ProcessType.clean_up
        ):
            print(
                "Task '{}' on '{}' not found on squeue; resetting the status "
                "to 'created' for resubmission".format(
                    const.ProcessType(db_task.proc_type).str_value, db_task.run_name
                )
            )
            shared.add_to_queue(
                queue_folder,
                db_task.run_name,
                db_task.proc_type,
                const.Status.created.value,
            )


def submit_task(
    sim_dir,
    proc_type,
    run_name,
    root_folder,
    queue_folder,
    binary_mode=True,
    rand_reset=True,
    hf_seed=None,
    extended_period=False,
    do_verification=False,
    models=None,
):
    # create the tmp folder
    # TODO: fix this issue
    sqlite_tmpdir = "/tmp/cer"
    if not os.path.exists(sqlite_tmpdir):
        os.makedirs(sqlite_tmpdir)

    # Metadata logging setup
    ch_log_dir = os.path.abspath(os.path.join(sim_dir, "ch_log"))
    if not os.path.isdir(ch_log_dir):
        os.mkdir(ch_log_dir)
    submitted_time = datetime.now().strftime(const.METADATA_TIMESTAMP_FMT)
    log_file = os.path.join(sim_dir, "ch_log", const.METADATA_LOG_FILENAME)

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
        )
        print("Submit EMOD3D arguments: ", args)
        submit_lf_main(args, est_model=models[0])
        store_metadata(
            log_file,
            const.ProcessType.EMOD3D.str_value,
            {"submit_time": submitted_time},
        )

    if proc_type == const.ProcessType.merge_ts.value:
        args = argparse.Namespace(
            auto=True,
            merge_ts=True,
            winbin_aio=False,
            srf=run_name,
            account=const.DEFAULT_ACCOUNT,
            machine=JOB_RUN_MACHINE[const.ProcessType.merge_ts].value,
            rel_dir=sim_dir,
            write_directory=sim_dir,
        )
        print("Submit post EMOD3D (merge_ts) arguments: ", args)
        submit_post_lf_main(args)
        store_metadata(
            log_file,
            const.ProcessType.merge_ts.str_value,
            {"submit_time": submitted_time},
        )

    if proc_type == const.ProcessType.winbin_aio.value:
        # skipping winbin_aio if running binary mode
        if binary_mode:
            # Update mgmt db
            shared.add_to_queue(
                queue_folder,
                run_name,
                proc_type,
                const.Status.completed.value,
                None,
                None,
            )
        else:
            args = argparse.Namespace(
                auto=True,
                winbin_aio=True,
                merge_ts=False,
                srf=run_name,
                account=const.DEFAULT_ACCOUNT,
                machine=JOB_RUN_MACHINE[const.ProcessType.winbin_aio].value,
            )
            print("Submit post EMOD3D (winbin_aio) arguments: ", args)
            submit_post_lf_main(args)
    if proc_type == const.ProcessType.HF.value:
        args = argparse.Namespace(
            auto=True,
            srf=run_name,
            ascii=not binary_mode,
            seed=hf_seed,
            rand_reset=rand_reset,
            ncore=const.HF_DEFAULT_NCORES,
            version=const.HF_DEFAULT_VERSION,
            site_specific=None,
            account=const.DEFAULT_ACCOUNT,
            machine=JOB_RUN_MACHINE[const.ProcessType.HF].value,
            rel_dir=sim_dir,
            write_directory=sim_dir,
            debug=False,
        )
        print("Submit HF arguments: ", args)
        submit_hf_main(args, models[1])
        store_metadata(
            log_file, const.ProcessType.HF.str_value, {"submit_time": submitted_time}
        )
    if proc_type == const.ProcessType.BB.value:
        args = argparse.Namespace(
            auto=True,
            srf=run_name,
            version=const.BB_DEFAULT_VERSION,
            account=const.DEFAULT_ACCOUNT,
            machine=JOB_RUN_MACHINE[const.ProcessType.BB].value,
            rel_dir=sim_dir,
            write_directory=sim_dir,
            ascii=False,
        )
        print("Submit BB arguments: ", args)
        submit_bb_main(args, models[2])
        store_metadata(
            log_file, const.ProcessType.BB.str_value, {"submit_time": submitted_time}
        )
    if proc_type == const.ProcessType.IM_calculation.value:
        options_dict = {
            SlBodyOptConsts.extended.value: True if extended_period else False,
            SlBodyOptConsts.simple_out.value: True,
            "auto": True,
            "machine": JOB_RUN_MACHINE[const.ProcessType.IM_calculation].value,
            "write_directory": sim_dir,
        }
        submit_im_calc_slurm(
            sim_dir=sim_dir, options_dict=options_dict, est_model=models[3]
        )
        print("Submit IM calc arguments: ", options_dict)
        store_metadata(
            log_file,
            const.ProcessType.IM_calculation.str_value,
            {"submit_time": submitted_time},
        )
    if do_verification:
        if proc_type == const.ProcessType.rrup.value:
            cmd = "sbatch $gmsim/workflow/scripts/calc_rrups_single.sl {} {}".format(
                sim_dir, root_folder
            )
            print(cmd)
            call(cmd, shell=True)

        if proc_type == const.ProcessType.Empirical.value:
            cmd = "$gmsim/workflow/scripts/submit_empirical.py -np 40 -i {} {}".format(
                run_name, root_folder
            )
            print(cmd)
            call(cmd, shell=True)

        if proc_type == const.ProcessType.Verification.value:
            pass
    if proc_type == const.ProcessType.clean_up.value:
        clean_up_template = (
            "--export=CUR_ENV -o {output_file} -e {error_file} {script_location} "
            "{sim_dir} {srf_name} {mgmt_db_loc} "
        )
        script = clean_up_template.format(
            sim_dir=sim_dir,
            srf_name=run_name,
            mgmt_db_loc=root_folder,
            script_location=os.path.expandvars("$gmsim/workflow/scripts/clean_up.sl"),
            output_file=os.path.join(sim_dir, "clean_up.out"),
            error_file=os.path.join(sim_dir, "clean_up.err"),
        )
        shared.submit_sl_script(
            script,
            const.ProcessType.clean_up.value,
            sim_struct.get_mgmt_db_queue(root_folder),
            run_name,
            submit_yes=True,
            target_machine=const.HPC.mahuika.value,
        )


def main(args):
    root_folder = args.root_folder
    queue_folder = sim_struct.get_mgmt_db_queue(root_folder)
    mgmt_db = MgmtDB(sim_struct.get_mgmt_db(root_folder))

    # Default values
    oneD_mod, hf_vs30_ref, binary_mode, hf_seed = DEFAULT_1D_MOD, None, True, None
    rand_reset, extended_period = True, False

    if args.config is not None:
        # parse and check for variables in config
        try:
            cybershake_cfg = ldcfg.load(
                directory=os.path.dirname(args.config),
                cfg_name=os.path.basename(args.config),
            )
            print("Cybershake config: \n", cybershake_cfg)
        except Exception as e:
            print("Error while parsing the config file, please double check inputs.")
            print(e)
            sys.exit()

        binary_mode = (
            cybershake_cfg["binary_mode"]
            if "binary_mode" in cybershake_cfg
            else binary_mode
        )
        hf_seed = cybershake_cfg["hf_seed"] if "hf_seed" in cybershake_cfg else hf_seed
        rand_reset = (
            cybershake_cfg["rand_reset"]
            if "rand_reset" in cybershake_cfg
            else rand_reset
        )
        extended_period = (
            cybershake_cfg["extended_period"]
            if "extended_period" in cybershake_cfg
            else extended_period
        )

    print("Loading estimation models")
    workflow_config = ldcfg.load()
    lf_est_model = est.load_full_model(
        os.path.join(workflow_config["estimation_models_dir"], "LF")
    )
    hf_est_model = est.load_full_model(
        os.path.join(workflow_config["estimation_models_dir"], "HF")
    )
    bb_est_model = est.load_full_model(
        os.path.join(workflow_config["estimation_models_dir"], "BB")
    )
    im_est_model = est.load_full_model(
        os.path.join(workflow_config["estimation_models_dir"], "IM")
    )

    # If any flags to ignore steps are given, add them to the list of skipped processes
    skipped = []
    if args.no_merge_ts:
        print("Not doing merge_ts")
        skipped.append(const.ProcessType.merge_ts.value)
    if args.no_im:
        print("Not calculating IMs")
        skipped.append(const.ProcessType.IM_calculation.value)
    if args.no_clean_up:
        print("Not cleaning up")
        skipped.append(const.ProcessType.clean_up.value)

    while True:
        # Get in progress tasks in the db and the HPC queue
        queue_tasks, n_tasks_to_run = [], {}
        for hpc in const.HPC:
            cur_tasks = get_queued_tasks(user=args.user, machine=hpc)
            n_tasks_to_run[hpc] = args.n_runs[hpc] - len(cur_tasks)
            queue_tasks.extend(cur_tasks)

        db_in_progress_tasks = mgmt_db.get_submitted_tasks()
        print("\nSqueue user tasks: ", ", ".join(queue_tasks))
        print(
            "In progress tasks in mgmt db:",
            ", ".join(
                [
                    "{}-{}-{}-{}".format(
                        entry.run_name,
                        const.ProcessType(entry.proc_type).str_value,
                        entry.job_id,
                        const.Status(entry.status).str_value,
                    )
                    for entry in db_in_progress_tasks
                ]
            ),
        )

        # Update the slurm mgmt based on squeue
        update_tasks(queue_folder, queue_tasks, db_in_progress_tasks)

        # Gets all runnable tasks based on mgmt db state
        runnable_tasks = mgmt_db.get_runnable_tasks(args.n_max_retries)
        print("Number of runnable tasks: ", len(runnable_tasks))

        # Select the first ntask_to_run that are not waiting
        # for mgmt db updates (i.e. items in the queue)
        tasks_to_run, task_counter = [], {key: 0 for key in const.HPC}
        for task in runnable_tasks[:100]:
            cur_run_name, cur_proc_type = task[1], task[0]

            # Set task that are set to be skipped to complete in db
            if cur_proc_type in skipped and not check_queue(
                queue_folder, cur_run_name, cur_proc_type
            ):
                shared.add_to_queue(
                    queue_folder,
                    cur_run_name,
                    cur_proc_type,
                    const.Status.completed.value,
                )
                continue

            cur_hpc = JOB_RUN_MACHINE[const.ProcessType(cur_proc_type)]
            # Add task if limit has not been reached and there are no
            # outstanding mgmt db updates
            if (
                not check_queue(queue_folder, cur_run_name, cur_proc_type)
                and task_counter.get(cur_hpc, 0) < n_tasks_to_run[cur_hpc]
            ):
                tasks_to_run.append(task)
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

        print(
            "Tasks to run this iteration: ",
            ", ".join(
                [
                    "{}-{}".format(entry[1], const.ProcessType(entry[0]).str_value)
                    for entry in tasks_to_run
                ]
            ),
            "\n",
        )

        # Submit the runnable tasks
        for task in tasks_to_run:
            proc_type, run_name, _ = task

            # Special handling for merge-ts
            if proc_type == const.ProcessType.merge_ts.value:
                # Check if clean up has already run
                if MgmtDB.is_task_complete(
                    [
                        const.ProcessType.clean_up.value,
                        run_name,
                        const.Status.completed.str_value,
                    ],
                    mgmt_db.get_runnable_tasks(args.n_max_retries),
                ):
                    # If clean_up has already run, then we should set it to
                    # be run again after merge_ts has run
                    shared.add_to_queue(
                        queue_folder,
                        run_name,
                        const.ProcessType.clean_up.value,
                        const.Status.created.value,
                    )

            # submit the job
            submit_task(
                sim_struct.get_sim_dir(root_folder, run_name),
                proc_type,
                run_name,
                root_folder,
                queue_folder,
                binary_mode=binary_mode,
                hf_seed=hf_seed,
                rand_reset=rand_reset,
                extended_period=extended_period,
                models=(lf_est_model, hf_est_model, bb_est_model, im_est_model),
            )

        print("Sleeping for {} second(s)".format(args.sleep_time))
        time.sleep(args.sleep_time)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("root_folder", type=str, help="The cybershake root folder")
    parser.add_argument(
        "--n_runs",
        default=None,
        type=list,
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
        "--n_max_retries",
        help="The maximum number of retries for any given task",
        default=DEFAULT_N_MAX_RETRIES,
        type=int,
    )
    parser.add_argument("--config", type=str, default=None, help="Cybershake config")
    parser.add_argument("--no_im", action="store_true")
    parser.add_argument("--no_merge_ts", action="store_true")
    parser.add_argument("--no_clean_up", action="store_true")

    args = parser.parse_args()

    if args.n_runs is not None:
        if len(args.n_runs) == 1:
            args.n_runs = {hpc: args.n_runs[0] for hpc in const.HPC}
        elif len(args.n_runs) == len(const.HPC):
            args.n_runs = {
                hpc: args.n_runs[index] for index, hpc in enumerate(const.HPC)
            }
        else:
            parser.error(
                "You must specify wither one common value for --n_runs, or one "
                "for each in the following list: {}".format(list(const.HPC))
            )
    else:
        args.n_runs = DEFAULT_N_RUNS

    main(args)
