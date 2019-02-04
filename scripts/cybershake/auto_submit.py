#!/usr/bin/env python3
"""Script for automatic submission of gm simulation jobs"""
import argparse
import os
import sys
from datetime import datetime
from subprocess import call

import shared_workflow.load_config as ldcfg
from scripts.management.db_helper import connect_db
from qcore.constants import ProcessType
from scripts.management import slurm_query_status, update_mgmt_db
from metadata.log_metadata import store_metadata, LOG_FILENAME
from scripts.submit_emod3d import main as submit_lf_main
from scripts.submit_post_emod3d import main as submit_post_lf_main
from scripts.submit_hf import main as submit_hf_main

default_n_runs = 12
default_1d_mod = "/nesi/transit/nesi00213/VelocityModel/Mod-1D/Cant1D_v2-midQ_leer.1d"


def submit_task(
    sim_dir,
    proc_type,
    run_name,
    db,
    mgmt_db_location,
    binary_mode=True,
    rand_reset=True,
    hf_seed=None,
    extended_period=False,
    do_verification=False,
):
    # TODO: using shell call is EXTREMELY undesirable. fix this in near future(fundamentally)
    # create the tmp folder
    # TODO: fix this issue
    sqlite_tmpdir = "/tmp/cer"
    if not os.path.exists(sqlite_tmpdir):
        os.makedirs(sqlite_tmpdir)

    # change the working directory to the sim_dir
    os.chdir(sim_dir)
    ch_log_dir = os.path.join(sim_dir, "ch_log")

    # create the folder if not exist
    if not os.path.isdir(ch_log_dir):
        os.mkdir(ch_log_dir)
    submitted_time = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(sim_dir, "ch_log", LOG_FILENAME)

    # LF
    params_uncertain_path = os.path.join(sim_dir, "LF", "params_uncertain.py")
    if proc_type == ProcessType.EMOD3D.value:
        check_params_uncertain(params_uncertain_path)
        args = argparse.Namespace(auto=True, srf=run_name)
        print("Submit EMOD3D arguments: ", args)
        submit_lf_main(args)
        store_metadata(
            log_file, ProcessType.EMOD3D.str_value, {"submit_time": submitted_time}
        )

    if proc_type == ProcessType.merge_ts.value:
        args = argparse.Namespace(auto=True, merge_ts=True, srf=run_name)
        print("Submit post EMOD3D (merge_ts) arguments: ", args)
        submit_post_lf_main(args)
        store_metadata(
            log_file, ProcessType.merge_ts.str_value, {"submit_time": submitted_time}
        )

    if proc_type == ProcessType.winbin_aio.value:
        # skipping winbin_aio if running binary mode
        if binary_mode:
            # update the mgmt_db
            update_mgmt_db.update_db(db, "winbin_aio", "completed", run_name=run_name)
        else:
            args = argparse.Namespace(auto=True, winbin_aio=True, srf=run_name)
            print("Submit post EMOD3D (winbin_aio) arguments: ", args)
            submit_post_lf_main(args)
    # HF
    if proc_type == ProcessType.HF.value:
        args = argparse.Namespace(
            auto=True,
            srf=run_name,
            ascii=not binary_mode,
            seed=hf_seed,
            rand_reset=rand_reset,
        )
        print("Submit HF arguments: ", args)
        submit_hf_main(args)
        store_metadata(
            log_file, ProcessType.HF.str_value, {"submit_time": submitted_time}
        )
    # BB
    if proc_type == ProcessType.BB.value:
        cmd = (
            "python $gmsim/workflow/scripts/submit_bb.py --binary --auto --srf %s"
            % run_name
        )
        print(cmd)
        call(cmd, shell=True)
        store_metadata(
            log_file, ProcTypeConst.BB.value, {"submit_time": submitted_time}
        )
    # IM_calc
    if proc_type == ProcessType.IM_calculation.value:
        cmd = "python $gmsim/workflow/scripts/submit_sim_imcalc.py --auto --sim_dir {} --simple_output {}".format(
            sim_dir, "-e" if extended_period else ""
        )
        print(cmd)
        call(cmd, shell=True)
        store_metadata(
            log_file, ProcTypeConst.IM.value, {"submit_time": submitted_time}
        )

    fault = run_name.split("_")[0]
    source_path = os.path.join(mgmt_db_location, "Data/Sources", fault, "srf", run_name)
    srf_path = source_path + ".srf"

    if do_verification:
        if proc_type == ProcessType.rrup.value:
            tmp_path = os.path.join(mgmt_db_location, "Runs")
            rrup_dir = os.path.join(mgmt_db_location, "Runs", fault, "verification")
            cmd = (
                "python $gmsim/workflow/scripts/submit_imcalc.py --auto -s --sim_dir %s --i %s --mgmt_db %s -srf %s -o %s"
                % (tmp_path, run_name, mgmt_db_location, srf_path, rrup_dir)
            )
            print(cmd)
            call(cmd, shell=True)

        if proc_type == ProcessType.Empirical.value:
            cmd = "$gmsim/workflow/scripts/submit_empirical.py -np 40 -i {} {}".format(
                run_name, mgmt_db_location
            )
            print(cmd)
            call(cmd, shell=True)

        if proc_type == ProcessType.Verification.value:
            pass

    # save the job meta data
    call(
        "echo 'submitted time: %s' >> %s"
        % (
            submitted_time,
            os.path.join(
                ch_log_dir, ProcessType(proc_type).name + ".%s.log" % run_name
            ),
        ),
        shell=True,
    )


def check_params_uncertain(params_uncertain_path):
    if not os.path.isfile(params_uncertain_path):
        print(params_uncertain_path, " missing, creating")
        cmd = "python $gmsim/workflow/scripts/submit_emod3d.py --set_params_only"
        call(cmd, shell=True)
        print(cmd)


def get_vmname(srf_name):
    """
        this function is mainly used for cybershake perpose
        get vm name from srf
        can be removed if mgmt_DB is updated to store vm name
    """
    vm_name = srf_name.split("_")[0]
    return vm_name


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("run_folder", type=str, help="folder to the collection of runs")
    parser.add_argument("--n_runs", default=default_n_runs, type=int)

    # cybershake-like simulations store mgmnt_db at different locations
    parser.add_argument("--single_sim", nargs="?", type=str, const=True)
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="a path to a config file that constains all the required values.",
    )
    parser.add_argument("--no_im", action="store_true")
    parser.add_argument("--user", type=str, default=None)

    args = parser.parse_args()

    n_runs_max = args.n_runs
    mgmt_db_location = args.run_folder
    db = connect_db(mgmt_db_location)

    # Default values
    oneD_mod, hf_vs30_ref, binary_mode, hf_seed = default_1d_mod, None, True, None
    rand_reset, extended_period = True, False

    if args.config is not None:
        # parse and check for variables in config
        try:
            cybershake_cfg = ldcfg.load(
                directory=os.path.dirname(args.config),
                cfg_name=os.path.basename(args.config),
            )
            print(cybershake_cfg)
        except Exception as e:
            print(e)
            print("Error while parsing the config file, please double check inputs.")
            sys.exit()

        oneD_mod = (
            cybershake_cfg["v_1d_mod"] if "v_1d_mod" in cybershake_cfg else oneD_mod
        )
        hf_vs30_ref = (
            cybershake_cfg["hf_stat_vs_ref"]
            if "hf_stat_vs_ref" in cybershake_cfg
            else hf_vs30_ref
        )
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

        # append more logic here if more variables are requested

    print("hf_seed", hf_seed)
    queued_tasks = slurm_query_status.get_queued_tasks()
    user_queued_tasks = slurm_query_status.get_queued_tasks(user=args.user).split("\n")
    db_tasks = slurm_query_status.get_submitted_db_tasks(db)
    print("queued task:", queued_tasks)
    print("subbed task:", db_tasks)
    slurm_query_status.update_tasks(db, queued_tasks, db_tasks)
    db_tasks = slurm_query_status.get_submitted_db_tasks(db)
    ntask_to_run = n_runs_max - len(user_queued_tasks)

    runnable_tasks = slurm_query_status.get_runnable_tasks(db, ntask_to_run)

    submit_task_count = 0
    task_num = 0
    print(submit_task_count)
    print(ntask_to_run)
    while (
        submit_task_count < ntask_to_run
        and submit_task_count < len(runnable_tasks)
        and task_num < len(runnable_tasks)
    ):
        db_task_status = runnable_tasks[task_num]
        proc_type = db_task_status[0]
        run_name = db_task_status[1]
        task_state = db_task_status[2]

        # skip im calcs if no_im == true
        if args.no_im and proc_type == 6:
            task_num = task_num + 1
            continue

        vm_name = run_name.split("_")[0]

        if args.single_sim:
            # TODO: if the directory changed, this may break. make this more robust
            sim_dir = mgmt_db_location
        else:
            # non-cybershake, db is the same loc as sim_dir
            sim_dir = os.path.join(mgmt_db_location, "Runs", vm_name, run_name)

        # submit the job
        submit_task(
            sim_dir,
            proc_type,
            run_name,
            db,
            mgmt_db_location,
            binary_mode=binary_mode,
            hf_seed=hf_seed,
            hf_vs30_ref=hf_vs30_ref,
            oneD_mod=oneD_mod,
            rand_reset=rand_reset,
            extended_period=extended_period,
        )

        submit_task_count = submit_task_count + 1
        task_num = task_num + 1

    db.connection.close()


if __name__ == "__main__":
    main()
