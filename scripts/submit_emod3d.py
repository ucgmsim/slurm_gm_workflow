#!/usr/bin/env python3
"""Script to create and submit a slurm script for LF

Needs to be python 2 and 3 compatible.
"""
# TODO: import the CONFIG here
# Section for parser to determine if using automate wct
import install
import argparse
from datetime import datetime

import set_runparams
import estimation.estimate_WC as wc

from qcore import utils
from management import db_helper
from management import update_mgmt_db
from shared_workflow.shared import *
from shared_workflow import load_config

# TODO: remove this once temp_shared is gone
from temp_shared import resolve_header

# Timestamp
timestamp_format = "%Y%m%d_%H%M%S"
timestamp = datetime.now().strftime(timestamp_format)

# Default values
default_core = "160"
default_run_time = "02:00:00"
default_memory = "16G"
default_account = 'nesi00213'
default_ch_scale = 1.1
default_wct_scale = 1.2

workflow_config = load_config.load(
    os.path.dirname(os.path.realpath(__file__)), "workflow_config.json")
global_root = workflow_config["global_root"]
tools_dir = os.path.join(global_root, 'opt/maui/emod3d/3.0.4-gcc/bin')


def confirm(q):
    show_horizontal_line()
    print(q)
    return show_yes_no_question()


def write_sl_script(
        lf_sim_dir, sim_dir, srf_name, mgmt_db_location, run_time=default_run_time,
        nb_cpus=default_core, memory=default_memory, account=default_account):
    set_runparams.create_run_params(srf_name)

    with open('run_emod3d.sl.template', 'r') as f:
        template = f.read()

    replace_t = [("{{lf_sim_dir}}", lf_sim_dir), ("{{tools_dir}}", tools_dir),
                 ("{{mgmt_db_location}}", mgmt_db_location),
                 ("{{sim_dir}}", sim_dir), ("{{srf_name}}", srf_name)]

    for pattern, value in replace_t:
        template = template.replace(pattern, value)

    # slurm header
    # TODO: this value has to change accordingly to the value used for WCT estimation
    job_name = "run_emod3d.%s" % srf_name
    header = resolve_header(
        account, nb_cpus, run_time, job_name, "slurm", memory, timestamp,
        job_description="emod3d slurm script",
        additional_lines="#SBATCH --hint=nomultithread")

    fname_slurm_script = 'run_emod3d_%s_%s.sl' % (srf_name, timestamp)
    with open(fname_slurm_script, 'w') as f:
        f.write(header)
        f.write(template)

    fname_sl_abs_path = os.path.join(os.path.abspath(os.path.curdir), fname_slurm_script)
    print("Slurm script %s written" % fname_sl_abs_path)

    return fname_sl_abs_path


if __name__ == '__main__':
    # Start of main function
    parser = argparse.ArgumentParser()
    parser.add_argument("--ncore", type=str, default=default_core)
    parser.add_argument("--auto", nargs="?", type=str, const=True)
    parser.add_argument('--account', type=str, default=default_account)
    parser.add_argument('--srf', type=str, default=None)
    parser.add_argument('--set_params_only', nargs="?", type=str, const=True)
    args = parser.parse_args()

    try:
        params = utils.load_params('root_params.yaml', 'fault_params.yaml', 'sim_params.yaml')
        utils.update(params, utils.load_params(os.path.join(params.vel_mod_dir, 'vm_params.yaml')))
    except Exception as e:
        print("Load params failed with exception: ", e)
        sys.exit(1)
    else:
        created_scripts = []
        if args.auto:
            submit_yes = True
        elif args.set_params_only:
            submit_yes = False
        else:
            submit_yes = confirm("Also submit the job for you?")

        print("params.srf_file", params.srf_file)
        wall_clock_limit = None
        for srf in params.srf_file:
            # Get the srf(rup) name without extensions
            srf_name = os.path.splitext(os.path.basename(srf))[0]
            # If srf(variation) is provided as args,
            # only create the slurm with same name provided
            if args.srf is not None and srf_name != args.srf:
                continue
            if args.set_params_only:
                set_runparams.create_run_params(srf_name)
                continue
            print("not set_params_only")
            # get lf_sim_dir
            lf_sim_dir = os.path.join(params.sim_dir, 'LF')
            sim_dir = params.sim_dir

            # default_core will be changed is user passes ncore
            num_procs = args.ncore
            if num_procs != default_core:
                print("Number of cores is different from default "
                      "number of cores. Estimation will be less accurate.")

            estimated_chours = wc.estimate_LF_WC_single(
                int(params.nx), int(params.ny), int(params.nz),
                int(params.sim_duration / params.dt), num_procs)
            print("Estimated WCT {}".format(
                wc.convert_to_wct(estimated_chours)))

            if args.auto:
                created_scripts = write_sl_script(
                    lf_sim_dir, sim_dir, srf_name, params.mgmt_db_location,
                    run_time=wc.get_wct(estimated_chours), nb_cpus=num_procs)
            else:
                # Get the wall clock time from the user
                if wall_clock_limit is None:
                    wall_clock_limit = str(install.get_input_wc())
                    print("WCT set to: %s" % wall_clock_limit)

                created_scripts = write_sl_script(
                    lf_sim_dir, sim_dir, srf_name, params.mgmt_db_location,
                    run_time=wall_clock_limit, nb_cpus=num_procs)

            # Submit for the user if specified
            jobid = None
            if submit_yes:
                print(created_scripts)
                if submit_yes:
                    print("Submitting %s" % created_scripts)
                    res = exe("sbatch %s" % created_scripts, debug=False)

                    if len(res[1]) == 0:            # No errors
                        # Get the jobid
                        jobid = res[0].split()[-1]

                        try:
                            int(jobid)
                        except ValueError:
                            print("{} is not a valid jobid".format(jobid))
                            sys.exit()

                        db = db_helper.connect_db(params.mgmt_db_location)
                        update_mgmt_db.update_db(
                            db, 'EMOD3D', 'queued',
                            job=jobid, run_name=srf_name)
                        db.connection.commit()
