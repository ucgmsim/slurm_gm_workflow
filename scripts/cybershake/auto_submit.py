#!/usr/bin/env python3
"""Script for automatic submission of gm simulation jobs"""
import argparse
import os
import sys
from datetime import datetime
from subprocess import call

import shared_workflow.load_config as ldcfg
from scripts.management.db_helper import connect_db
import qcore.constants as const
from scripts.management import slurm_query_status, update_mgmt_db
from metadata.log_metadata import store_metadata

from scripts.submit_emod3d import main as submit_lf_main
from scripts.submit_post_emod3d import main as submit_post_lf_main
from scripts.submit_hf import main as submit_hf_main
from scripts.submit_bb import main as submit_bb_main
from scripts.submit_sim_imcalc import submit_im_calc_slurm, SlBodyOptConsts
from scripts.clean_up import clean_up_submission_lf_files
from shared_workflow import shared

default_n_runs = 12
default_1d_mod = "/nesi/transit/nesi00213/VelocityModel/Mod-1D/Cant1D_v2-midQ_leer.1d"

job_run_machine = {
    const.ProcessType.EMOD3D.value: const.HPC.maui.value,
    const.ProcessType.merge_ts.value: const.HPC.mahuika.value,
    const.ProcessType.winbin_aio.value: const.HPC.maui.value,
    const.ProcessType.HF.value: const.HPC.maui.value,
    const.ProcessType.BB.value: const.HPC.maui.value,
    const.ProcessType.IM_calculation.value: const.HPC.maui.value,
}


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
    # create the tmp folder
    # TODO: fix this issue
    sqlite_tmpdir = "/tmp/cer"
    if not os.path.exists(sqlite_tmpdir):
        os.makedirs(sqlite_tmpdir)

    ch_log_dir = os.path.abspath(os.path.join(sim_dir, "ch_log"))

    # create the folder if not exist
    if not os.path.isdir(ch_log_dir):
        os.mkdir(ch_log_dir)
    submitted_time = datetime.now().strftime(const.METADATA_TIMESTAMP_FMT)
    log_file = os.path.join(sim_dir, "ch_log", const.METADATA_LOG_FILENAME)

    # LF
    # params_uncertain_path = os.path.join(sim_dir, "LF", "params_uncertain.py")
    if proc_type == const.ProcessType.EMOD3D.value:
        # check_params_uncertain(params_uncertain_path)

        # These have to include the default values (same for all other process types)!
        args = argparse.Namespace(
            auto=True,
            srf=run_name,
            ncore=const.LF_DEFAULT_NCORES,
            account=const.DEFAULT_ACCOUNT,
            machine=job_run_machine[const.ProcessType.EMOD3D.value],
            rel_dir=sim_dir,
            write_directory=sim_dir,
        )
        print("Submit EMOD3D arguments: ", args)
        submit_lf_main(args)
        store_metadata(
            log_file,
            const.ProcessType.EMOD3D.str_value,
            {"submit_time": submitted_time},
        )

    # Merge ts
    if proc_type == const.ProcessType.merge_ts.value:
        args = argparse.Namespace(
            auto=True,
            merge_ts=True,
            winbin_aio=False,
            srf=run_name,
            account=const.DEFAULT_ACCOUNT,
            machine=job_run_machine[const.ProcessType.merge_ts.value],
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
            # update the mgmt_db
            update_mgmt_db.update_db(
                db, "winbin_aio", const.State.completed.value[1], run_name=run_name
            )
        else:
            args = argparse.Namespace(
                auto=True,
                winbin_aio=True,
                merge_ts=False,
                srf=run_name,
                account=const.DEFAULT_ACCOUNT,
                machine=job_run_machine[const.ProcessType.winbin_aio.value],
            )
            print("Submit post EMOD3D (winbin_aio) arguments: ", args)
            submit_post_lf_main(args)
    # HF
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
            machine=job_run_machine[const.ProcessType.HF.value],
            rel_dir=sim_dir,
            write_directory=sim_dir,
            debug=False,
        )
        print("Submit HF arguments: ", args)
        submit_hf_main(args)
        store_metadata(
            log_file, const.ProcessType.HF.str_value, {"submit_time": submitted_time}
        )
    # BB
    if proc_type == const.ProcessType.BB.value:
        args = argparse.Namespace(
            auto=True,
            srf=run_name,
            version=const.BB_DEFAULT_VERSION,
            account=const.DEFAULT_ACCOUNT,
            machine=job_run_machine[const.ProcessType.BB.value],
            rel_dir=sim_dir,
            write_directory=sim_dir,
            ascii=False,
        )
        print("Submit BB arguments: ", args)
        submit_bb_main(args)
        store_metadata(
            log_file, const.ProcessType.BB.str_value, {"submit_time": submitted_time}
        )
    # IM_calc
    if proc_type == const.ProcessType.IM_calculation.value:
        options_dict = {
            SlBodyOptConsts.extended.value: True if extended_period else False,
            SlBodyOptConsts.simple_out.value: True,
            "auto": True,
            "machine": job_run_machine[const.ProcessType.IM_calculation.value],
            "write_directory": sim_dir,
        }
        submit_im_calc_slurm(sim_dir=sim_dir, options_dict=options_dict)
        print("Submit IM calc arguments: ", options_dict)
        store_metadata(
            log_file,
            const.ProcessType.IM_calculation.str_value,
            {"submit_time": submitted_time},
        )

    fault = run_name.split("_")[0]

    if do_verification:
        if proc_type == const.ProcessType.rrup.value:
            realisation_directory = os.path.join(
                mgmt_db_location, "Runs", fault, run_name
            )
            cmd = "sbatch $gmsim/workflow/scripts/calc_rrups_single.sl {} {}".format(
                realisation_directory, mgmt_db_location
            )
            print(cmd)
            call(cmd, shell=True)

        if proc_type == const.ProcessType.Empirical.value:
            cmd = "$gmsim/workflow/scripts/submit_empirical.py -np 40 -i {} {}".format(
                run_name, mgmt_db_location
            )
            print(cmd)
            call(cmd, shell=True)

        if proc_type == const.ProcessType.Verification.value:
            pass

    if proc_type == const.ProcessType.clean_up.value:
        clean_up_template = (
            "--export=gmsim -o {output_file} -e {error_file} {script_location} "
            "{sim_dir} {srf_name} {mgmt_db_loc} "
        )
        script = clean_up_template.format(
            sim_dir=sim_dir,
            srf_name=run_name,
            mgmt_db_loc=mgmt_db_location,
            script_location=os.path.expandvars("$gmsim/workflow/scripts/clean_up.sl"),
            output_file=os.path.join(sim_dir, "clean_up.out"),
            error_file=os.path.join(sim_dir, "clean_up.err"),
        )
        shared.submit_sl_script(
            script,
            const.ProcessType.clean_up.str_value,
            const.State.queued.value[1],
            mgmt_db_location,
            run_name,
            submitted_time,
            submit_yes=True,
            target_machine=const.HPC.mahuika.value,
        )


# TODO: Requires updating, currently not working
# def check_params_uncertain(params_uncertain_path):
#     if not os.path.isfile(params_uncertain_path):
#         print(params_uncertain_path, " missing, creating")
#         cmd = "python $gmsim/workflow/scripts/submit_emod3d.py --set_params_only"
#         call(cmd, shell=True)
#         print(cmd)


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
    parser.add_argument("--no_merge_ts", action="store_true")
    parser.add_argument("--user", type=str, default=None)
    parser.add_argument("--no_tidy_up", action="store_true")

    args = parser.parse_args()

    n_runs_max = args.n_runs
    mgmt_db_location = args.run_folder
    db = connect_db(mgmt_db_location)
    tidy_up = not args.no_tidy_up

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
    # db_tasks = slurm_query_status.get_submitted_db_tasks(db)
    ntask_to_run = n_runs_max - len(user_queued_tasks)

    runnable_tasks = slurm_query_status.get_runnable_tasks(db, ntask_to_run)
    print("runnable task:")
    print(runnable_tasks)
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
        if args.no_im and proc_type == const.ProcessType.IM_calculation.value:
            task_num = task_num + 1
            continue
        if proc_type == const.ProcessType.merge_ts.value:
            if args.no_merge_ts:
                update_mgmt_db.update_db(
                    db, "merge_ts", const.State.completed.value[1], run_name=run_name
                )
                task_num = task_num + 1
                continue
            elif slurm_query_status.is_task_complete(
                [
                    const.ProcessType.clean_up.value,
                    run_name,
                    const.State.completed.value[1],
                ],
                slurm_query_status.get_db_tasks_to_be_run(db),
            ):
                # If clean_up has already run, then we should set it to be run again after merge_ts has run
                update_mgmt_db.update_db(
                    db,
                    const.ProcessType.clean_up.str_value,
                    const.State.created.value[1],
                    run_name=run_name,
                )
        if not tidy_up and proc_type == const.ProcessType.clean_up.value:
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
            rand_reset=rand_reset,
            extended_period=extended_period,
        )

        submit_task_count = submit_task_count + 1
        task_num = task_num + 1

    db.connection.close()


if __name__ == "__main__":
    main()
