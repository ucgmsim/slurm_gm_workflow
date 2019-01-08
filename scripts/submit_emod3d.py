#!/usr/bin/env python3
"""Script to create and submit a slurm script for LF"""
# TODO: import the CONFIG here
# Section for parser to determine if using automate wct
import install
import argparse
from datetime import datetime

import set_runparams
import estimation.estimate_WC as wc

from qcore import utils
from shared_workflow.shared import *
from shared_workflow import load_config

# TODO: remove this once temp_shared is gone
from temp_shared import resolve_header

# Timestamp
timestamp_format = "%Y%m%d_%H%M%S"
timestamp = datetime.now().strftime(timestamp_format)

# Default values
default_core = 160
default_run_time = "02:00:00"
default_memory = "16G"
default_account = 'nesi00213'

workflow_config = load_config.load(
    os.path.dirname(os.path.realpath(__file__)), "workflow_config.json")
global_root = workflow_config["global_root"]
tools_dir = os.path.join(global_root, 'opt/maui/emod3d/3.0.4-gcc/bin')


def write_sl_script(
        lf_sim_dir, sim_dir, srf_name, mgmt_db_location, run_time=default_run_time,
        nb_cpus=default_core, memory=default_memory, account=default_account):
    set_runparams.create_run_params(srf_name)
    """Populates the template and writes the resulting slurm script to file"""

    with open('run_emod3d.sl.template', 'r') as f:
        template = f.read()

    replace_t = [("{{lf_sim_dir}}", lf_sim_dir), ("{{tools_dir}}", tools_dir),
                 ("{{mgmt_db_location}}", mgmt_db_location),
                 ("{{sim_dir}}", sim_dir), ("{{srf_name}}", srf_name)]

    for pattern, value in replace_t:
        template = template.replace(pattern, value)

    # slurm header
    job_name = "run_emod3d.%s" % srf_name
    header = resolve_header(
        account, str(nb_cpus), run_time, job_name, "slurm", memory, timestamp,
        job_description="emod3d slurm script",
        additional_lines="#SBATCH --hint=nomultithread")

    fname_slurm_script = 'run_emod3d_%s_%s.sl' % (srf_name, timestamp)
    with open(fname_slurm_script, 'w') as f:
        f.write(header)
        f.write(template)

    fname_sl_abs_path = os.path.join(os.path.abspath(os.path.curdir),
                                     fname_slurm_script)
    print("Slurm script %s written" % fname_sl_abs_path)

    return fname_sl_abs_path


if __name__ == '__main__':
    # Start of main function
    parser = argparse.ArgumentParser(
        description="Create (and submit if specified) the slurm script for LF")

    parser.add_argument("--ncore", type=int, default=default_core)
    parser.add_argument("--auto", nargs="?", type=str, const=True)
    parser.add_argument('--account', type=str, default=default_account)
    parser.add_argument('--srf', type=str, default=None)
    parser.add_argument('--set_params_only', nargs="?", type=str, const=True)
    args = parser.parse_args()

    try:
        params = utils.load_params(
            'root_params.yaml', 'fault_params.yaml', 'sim_params.yaml')
        utils.update(params, utils.load_params(
            os.path.join(params.vel_mod_dir, 'vm_params.yaml')))
    except Exception as e:
        print("Load params failed with exception: ", e)
        sys.exit(1)

    if args.auto:
        submit_yes = True
    elif args.set_params_only:
        submit_yes = False
    else:
        submit_yes = confirm("Also submit the job for you?")

    print("params.srf_file", params.srf_file)
    wall_clock_limit = None
    # Get the srf(rup) name without extensions
    srf_name = os.path.splitext(os.path.basename(params.srf_file))[0]
    if args.set_params_only:
        set_runparams.create_run_params(srf_name)
    elif args.srf is None or srf_name == args.srf:
        print("not set_params_only")
        # get lf_sim_dir
        lf_sim_dir = os.path.join(params.sim_dir, 'LF')
        sim_dir = params.sim_dir

        ncores = args.ncore
        est_core_hours, est_run_time, ncores = wc.estimate_LF_WC_single(
            int(params.nx), int(params.ny), int(params.nz),
            int(float(params.sim_duration) / float(params.dt)), ncores,
            True)
        print("Estimated WCT {} with {} cores".format(
            wc.convert_to_wct(est_run_time), ncores))

        if args.auto:
            script = write_sl_script(
                lf_sim_dir, sim_dir, srf_name, params.mgmt_db_location,
                run_time=wc.get_wct(est_run_time), nb_cpus=ncores)
        else:
            # Get the wall clock time from the user
            if wall_clock_limit is None:
                print("Use the estimated wall clock time? (Minimum of "
                      "5 mins, otherwise adds a 10% overestimation to "
                      "ensure the job completes)")
                use_estimation = show_yes_no_question()
                if use_estimation:
                    wall_clock_limit = wc.get_wct(est_run_time)
                else:
                    wall_clock_limit = str(install.get_input_wc())
                print("WCT set to: %s" % wall_clock_limit)

            script = write_sl_script(
                lf_sim_dir, sim_dir, srf_name, params.mgmt_db_location,
                run_time=wall_clock_limit, nb_cpus=ncores)

        if args.auto:
            submit_yes = True
        elif args.set_params_only:
            submit_yes = False
        else:
            submit_yes = confirm("Also submit the job for you?")

        submit_sl_script(script, 'EMOD3D', 'queued',
                         params.mgmt_db_location, srf_name, submit_yes)
