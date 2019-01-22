from scripts.management import db_helper
from scripts.management import slurm_query_status
from scripts.management import update_mgmt_db
from subprocess import call

import shared_workflow.load_config as ldcfg

from datetime import datetime
import time

import argparse
import os
import sys

default_binary_mode = True
default_n_runs = 12
default_1d_mod = "/nesi/transit/nesi00213/VelocityModel/Mod-1D/Cant1D_v2-midQ_leer.1d"
default_hf_vs30_ref = None
default_hf_seed = None
default_rand_reset = True
default_extended_period = False


def submit_task(sim_dir, proc_type, run_name, db, mgmt_db_location, binary_mode=True, rand_reset=default_rand_reset,
                hf_seed=None, extended_period=default_extended_period):
    # TODO: using shell call is EXTREMELY undesirable. fix this in near future(fundamentally)

    # create the tmp folder
    # TODO: fix this issue
    sqlite_tmpdir = '/tmp/cer'
    if not os.path.exists(sqlite_tmpdir):
        os.makedirs(sqlite_tmpdir)

    # change the working directory to the sim_dir
    os.chdir(sim_dir)
    ch_log_dir = os.path.join(sim_dir, 'ch_log')
    # create the folder if not exsist
    if not os.path.isdir(ch_log_dir):
        os.mkdir(ch_log_dir)
    submitted_time = datetime.now().strftime('%Y%m%d_%H%M%S')
    #    print "sim_dir:%s"%sim_dir
    # idenfity the proc_type, EMOD3D:1, merge_ts:2, winbin_aio:3, HF:4, BB:5

    if proc_type == 1:
        # EMOD 3D
        cmd = "python $gmsim/workflow/scripts/submit_emod3d.py --auto --srf %s" % run_name
        print(cmd)
        call(cmd, shell=True)
        # save meta, TODO:replace proper db update when merge
        call("echo 'submitted time: %s' >> %s" % (submitted_time, os.path.join(ch_log_dir, 'EMOD3D.%s.log' % run_name)),
             shell=True)

    if proc_type == 2:
        cmd = "python $gmsim/workflow/scripts/submit_post_emod3d.py --auto --merge_ts --srf %s" % run_name
        print(cmd)
        call(cmd, shell=True)
        call("echo 'submitted time: %s' >> %s" % (
        submitted_time, os.path.join(ch_log_dir, 'post_emod.%s.log' % run_name)), shell=True)

    if proc_type == 3:
        # skipping winbin_aio if running binary mode
        if binary_mode == True:
            # update the mgmt_db
            update_mgmt_db.update_db(db, 'winbin_aio', 'completed', run_name=run_name)
        else:
            cmd = "python $gmsim/workflow/scripts/submit_post_emod3d.py --auto --winbin_aio --srf %s" % run_name
            print(cmd)
            call(cmd, shell=True)
            call("echo 'submitted time: %s' >> %s" % (
            submitted_time, os.path.join(ch_log_dir, 'winbin.%s.log' % run_name)), shell=True)

    if proc_type == 4:
    #     # run the submit_post_emod3d before install_bb and submit_hf
    #     # TODO: fix this strange logic in the actual workflow
    #     if default_hf_vs30_ref != None:
    #         cmd = "python $gmsim/workflow/scripts/install_bb.py --v1d %s --hf_stat_vs_ref %s" % (
    #         default_1d_mod, default_hf_vs30_ref)
    #         print(cmd)
    #         call(cmd, shell=True)
    #     else:
    #         cmd = "python $gmsim/workflow/scripts/install_bb.py --v1d %s" % default_1d_mod
    #         print(cmd)
    #         call(cmd, shell=True)
        hf_cmd = "python $gmsim/workflow/scripts/submit_hf.py --binary --auto --srf %s" % run_name
        if hf_seed is not None:
            hf_cmd = "{} --seed {}".format(hf_cmd, hf_seed)
        if rand_reset:
            hf_cmd = "{} --rand_reset".format(hf_cmd)
        print(hf_cmd)
        call(hf_cmd, shell=True)
        call("echo 'submitted time: %s' >> %s" % (submitted_time, os.path.join(ch_log_dir, 'HF.%s.log' % run_name)),
             shell=True)

    if proc_type == 5:
        cmd = "python $gmsim/workflow/scripts/submit_bb.py --binary --auto --srf %s" % run_name
        print(cmd)
        call(cmd, shell=True)
        call("echo 'submitted time: %s' >> %s" % (submitted_time, os.path.join(ch_log_dir, 'BB.%s.log' % run_name)),
             shell=True)

    if proc_type == 6:
        # TODO: fix inconsistant naming in sub_imcalc.py
        tmp_path = os.path.join(mgmt_db_location, 'Runs')
        cmd = "python $gmsim/workflow/scripts/submit_imcalc.py --auto --sim_dir %s --i %s --mgmt_db %s --simple_output" % (
        tmp_path, run_name, mgmt_db_location)
        if extended_period == True:
            cmd = cmd + ' -e'
        print(cmd)
        call(cmd, shell=True)
        # save the job meta data
        call(
            "echo 'submitted time: %s' >> %s" % (submitted_time, os.path.join(ch_log_dir, 'IM_calc.%s.log' % run_name)),
            shell=True)


def get_vmname(srf_name):
    """
        this function is mainly used for cybershake perpose
        get vm name from srf
        can be removed if mgmt_DB is updated to store vm name
    """
    vm_name = srf_name.split('_')[0]
    return vm_name


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('run_folder', type=str, help="folder to the collection of runs on Kupe")
    parser.add_argument('--n_runs', default=default_n_runs, type=int)
    # cybershake-like simulations store mgmnt_db at different locations
    parser.add_argument('--single_sim', nargs="?", type=str, const=True)
    parser.add_argument('--config', type=str, default=None,
                        help="a path to a config file that constains all the required values.")
    parser.add_argument('--no_im', action="store_true")
    parser.add_argument('--user', type=str, default=None)

    args = parser.parse_args()
    mgmt_db_location = args.run_folder
    n_runs_max = args.n_runs
    db = db_helper.connect_db(mgmt_db_location)
    db_tasks = []
    hf_seed = default_hf_seed

    if args.config != None:
        # parse and check for variables in config
        try:
            cybershake_cfg = ldcfg.load(directory=os.path.dirname(args.config), cfg_name=os.path.basename(args.config))
            print(cybershake_cfg)
        except Exception as e:
            print(e)
            print("Error while parsing the config file, please double check inputs.")
            sys.exit()
        if 'v_1d_mod' in cybershake_cfg:
            # TODO:bad hack, fix this when possible (with parsing)
            global default_1d_mod
            default_1d_mod = cybershake_cfg['v_1d_mod']
        if 'hf_stat_vs_ref' in cybershake_cfg:
            # TODO:bad hack, fix this when possible (with parsing)
            global default_hf_vs30_ref
            default_hf_vs30_ref = cybershake_cfg['hf_stat_vs_ref']
        if 'binary_mode' in cybershake_cfg:
            binary_mode = cybershake_cfg['binary_mode']
        else:
            binary_mode = default_binary_mode

        if 'hf_seed' in cybershake_cfg:
            hf_seed = cybershake_cfg['hf_seed']

        if 'rand_reset' in cybershake_cfg:
            rand_reset = cybershake_cfg['rand_reset']
        else:
            rand_reset = default_rand_reset

        if 'extended_period' in cybershake_cfg:
            extended_period = cybershake_cfg['extended_period']
        else:
            extended_period = default_extended_period
            # append more logic here if more variables are requested

    print("hf_seed:", hf_seed)
    queued_tasks = slurm_query_status.get_queued_tasks()
    user_queued_tasks = slurm_query_status.get_queued_tasks(user=args.user).split('\n')
    db_tasks = slurm_query_status.get_submitted_db_tasks(db)
    print('queued task:', queued_tasks)
    print('subbed task:', db_tasks)
    slurm_query_status.update_tasks(db, queued_tasks, db_tasks)
    db_tasks = slurm_query_status.get_submitted_db_tasks(db)
    # submitted_tasks = slurm_query_status.get_submitted_db_tasks(db)
    # ntask_to_run = n_runs_max - len(db_tasks)
    ntask_to_run = n_runs_max - len(user_queued_tasks)

    runnable_tasks = slurm_query_status.get_runnable_tasks(db, ntask_to_run)

    submit_task_count = 0
    task_num = 0
    print(submit_task_count)
    print(ntask_to_run)
    while submit_task_count < ntask_to_run and submit_task_count < len(runnable_tasks) and task_num < len(
            runnable_tasks):
        db_task_status = runnable_tasks[task_num]

        proc_type = db_task_status[0]
        run_name = db_task_status[1]
        task_state = db_task_status[2]

        # skip im calcs if no_im == true
        if args.no_im and proc_type == 6:
            task_num = task_num + 1
            continue

        vm_name = get_vmname(run_name)

        if args.single_sim == True:
            # TODO: if the directory changed, this may break. make this more robust
            sim_dir = mgmt_db_location
        else:
            # non-cybershake, db is the same loc as sim_dir
            sim_dir = os.path.join(mgmt_db_location, "Runs", vm_name, run_name)
        # submit the job
        submit_task(sim_dir, proc_type, run_name, db, mgmt_db_location, binary_mode, rand_reset, hf_seed,
                    extended_period)

        submit_task_count = submit_task_count + 1
        task_num = task_num + 1
        # a sleep between cmds
        # time.sleep(5)
    db.connection.close()


if __name__ == '__main__':
    main()
