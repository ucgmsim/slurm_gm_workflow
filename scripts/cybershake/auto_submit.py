import argparse
import os
import sys
from datetime import datetime
from subprocess import call

import shared_workflow.load_config as ldcfg
from scripts.management import db_helper, slurm_query_status, update_mgmt_db
from scripts.submit_emod3d import main as submit_lf_main
from scripts.submit_post_emod3d import main as submit_post_lf_main

default_n_runs = 12
default_1d_mod = "/nesi/transit/nesi00213/VelocityModel/Mod-1D/Cant1D_v2-midQ_leer.1d"


def submit_task(
    sim_dir,
    proc_type,
    run_name,
    db,
    mgmt_db_location,
    binary_mode=True,
    hf_seed=None,
    hf_vs30_ref=None,
    oneD_mod=default_1d_mod,
    rand_reset=True,
    extended_period=False,
):
    # create the tmp folder
    # TODO: fix this issue
    sqlite_tmpdir = "/tmp/cer"
    if not os.path.exists(sqlite_tmpdir):
        os.makedirs(sqlite_tmpdir)

    # change the working directory to the sim_dir
    os.chdir(sim_dir)
    ch_log_dir = os.path.join(sim_dir, "ch_log")
    # create the folder if it does not exist
    if not os.path.isdir(ch_log_dir):
        os.mkdir(ch_log_dir)
    submitted_time = datetime.now().strftime("%Y%m%d_%H%M%S")

    # EMOD 3D
    if proc_type == 1:
        args =  argparse.Namespace(auto=True, srf=run_name)
        print("Submit EMOD3D arguments: ", args)
        submit_lf_main(args)

        # save meta, TODO:replace proper db update when merge
        call(
            "echo 'submitted time: %s' >> %s"
            % (submitted_time, os.path.join(ch_log_dir, "EMOD3D.%s.log" % run_name)),
            shell=True,
        )

    # Post EMOD 3D
    if proc_type == 2:
        args = argparse.Namespace(auto=True, merge_ts=True, srf=run_name)
        print("Submit post EMOD3D (merge_ts) arguments: ", args)
        submit_post_lf_main(args)

        call(
            "echo 'submitted time: %s' >> %s"
            % (submitted_time, os.path.join(ch_log_dir, "post_emod.%s.log" % run_name)),
            shell=True,
        )

    if proc_type == 3:
        # skipping winbin_aio if running binary mode
        if binary_mode:
            update_mgmt_db.update_db(db, "winbin_aio", "completed", run_name=run_name)
        else:
            args = argparse.Namespace(auto=True, winbin_aio=True, srf=run_name)
            print("Submit post EMOD3D (winbin_aio) arguments: ", args)
            submit_post_lf_main(args)

            call(
                "echo 'submitted time: %s' >> %s"
                % (
                    submitted_time,
                    os.path.join(ch_log_dir, "winbin.%s.log" % run_name),
                ),
                shell=True,
            )

    if proc_type == 4:
        # run the submit_post_emod3d before install_bb and submit_hf
        # TODO: fix this strange logic in the actual workflow
        if oneD_mod != None:
            cmd = (
                "python $gmsim/workflow/scripts/install_bb.py --v1d %s --hf_stat_vs_ref %s"
                % (oneD_mod, hf_vs30_ref)
            )
            print(cmd)
            call(cmd, shell=True)
        else:
            cmd = "python $gmsim/workflow/scripts/install_bb.py --v1d %s" % oneD_mod
            print(cmd)
            call(cmd, shell=True)
        hf_cmd = (
            "python $gmsim/workflow/scripts/submit_hf.py --binary --auto --srf %s"
            % run_name
        )
        if hf_seed is not None:
            hf_cmd = "{} --seed {}".format(hf_cmd, hf_seed)
        if rand_reset:
            hf_cmd = "{} --rand_reset".format(hf_cmd)
        print(hf_cmd)
        call(hf_cmd, shell=True)
        call(
            "echo 'submitted time: %s' >> %s"
            % (submitted_time, os.path.join(ch_log_dir, "HF.%s.log" % run_name)),
            shell=True,
        )

    if proc_type == 5:
        cmd = (
            "python $gmsim/workflow/scripts/submit_bb.py --binary --auto --srf %s"
            % run_name
        )
        print(cmd)
        call(cmd, shell=True)
        call(
            "echo 'submitted time: %s' >> %s"
            % (submitted_time, os.path.join(ch_log_dir, "BB.%s.log" % run_name)),
            shell=True,
        )

    if proc_type == 6:
        # TODO: fix inconsistant naming in sub_imcalc.py
        tmp_path = os.path.join(mgmt_db_location, "Runs")
        cmd = (
            "python $gmsim/workflow/scripts/submit_imcalc.py --auto --sim_dir %s --i %s --mgmt_db %s --simple_output"
            % (tmp_path, run_name, mgmt_db_location)
        )
        if extended_period == True:
            cmd = cmd + " -e"
        print(cmd)
        call(cmd, shell=True)
        # save the job meta data
        call(
            "echo 'submitted time: %s' >> %s"
            % (submitted_time, os.path.join(ch_log_dir, "IM_calc.%s.log" % run_name)),
            shell=True,
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "run_folder", type=str, help="folder to the collection of runs on Maui"
    )
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
    db = db_helper.connect_db(mgmt_db_location)

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

    print("hf_seed:", hf_seed)
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
