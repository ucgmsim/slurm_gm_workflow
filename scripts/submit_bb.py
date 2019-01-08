#!/usr/bin/env python3
"""Script to create and submit a slurm script for BB"""
import argparse

import install
import estimation.estimate_WC as wc

from datetime import datetime

# TODO: move this to qcore library
from qcore import shared
from temp_shared import resolve_header
from shared_workflow.shared import *

from management import update_mgmt_db
from management import db_helper

from qcore import utils

sys.path.append(os.path.abspath(os.path.curdir))

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

default_account = 'nesi00213'
default_version = 'run_bb_mpi'
default_core = 80
default_wct = "00:30:00"
default_memory = "16G"

params = utils.load_params('root_params.yaml', 'fault_params.yaml',
                           'sim_params.yaml')
utils.update(params, utils.load_params(
    os.path.join(params.vel_mod_dir, 'vm_params.yaml')))


def write_sl_script(bb_sim_dir, sim_dir, srf_name, sl_template_prefix,
                    nb_cpus=default_core, run_time=default_wct,
                    memory=default_memory, account=default_account,
                    binary=False):
    with open('%s.sl.template' % sl_template_prefix, 'r') as f:
        template = f.read()

    if binary:
        create_directory = "mkdir -p " + os.path.join(bb_sim_dir, "Acc") + "\n"
        submit_command = create_directory + "srun python $gmsim/workflow" \
                                            "/scripts/bb_sim.py "
        arguments = [os.path.join(sim_dir, 'LF', "OutBin"), params.vel_mod_dir,
                     os.path.join(sim_dir, 'HF', "Acc/HF.bin"),
                     params.stat_vs_est, os.path.join(bb_sim_dir, "Acc/BB.bin"),
                     "--flo", str(params.flo)]
        template = template.replace(
            "{{bb_submit_command}}", submit_command + " ".join(arguments))
    else:
        template = template.replace(
            "{{bb_submit_command}}", "srun python  $gmsim/workflow/scripts"
                                     "/match_seismo-mpi.py " + bb_sim_dir)

    variation = srf_name.replace('/', '__')
    print(variation)

    print("sim dir, srf_name", sim_dir, srf_name)
    replace_t = [("$rup_mod", variation),
                 ("{{mgmt_db_location}}", params.mgmt_db_location),
                 ("{{sim_dir}}", sim_dir), ("{{srf_name}}", srf_name),
                 ("{{test_bb_script}}",
                  "test_bb_binary.sh" if binary else "test_bb_ascii.sh")]

    for pattern, value in replace_t:
        template = template.replace(pattern, value)

    job_name = "sim_bb_%s" % variation
    header = resolve_header(account, str(nb_cpus), run_time, job_name, "slurm",
                            memory, timestamp, job_description="BB calculation",
                            additional_lines="##SBATCH -C avx")
    fname_sl_script = '%s_%s_%s.sl' % (sl_template_prefix, variation, timestamp)
    with open(fname_sl_script, 'w') as f:
        f.write(header)
        f.write(template)

    fname_sl_abs_path = os.path.join(
        os.path.abspath(os.path.curdir), fname_sl_script)
    print("Slurm script %s written" % fname_sl_abs_path)
    return fname_sl_abs_path


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Create (and submit if specified) the slurm script for BB")

    parser.add_argument("--auto", nargs="?", type=str, const=True)
    parser.add_argument('--version', type=str, default=default_version)
    parser.add_argument('--account', type=str, default=default_account)
    parser.add_argument('--srf', type=str, default=None)
    parser.add_argument('--ascii', action="store_true", default=False)
    args = parser.parse_args()

    ncores = default_core
    if args.version is not None:
        version = args.version
        if version == 'serial' or version == 'run_bb':
            sl_name_prefix = 'run_bb'
            ncores = 1
        elif version == 'mp' or version == 'run_bb_mp':
            sl_name_prefix = 'run_bb_mp'
        elif version == 'mpi' or version == 'run_bb_mpi':
            sl_name_prefix = 'run_bb_mpi'
        else:
            print('% cannot be recognize as a valide option' % version)
            print('version is set to default: %', default_version)
            version = default_version
            sl_name_prefix = default_version
    else:
        version = default_version
        sl_name_prefix = default_version
    print(version)

    # est_wct and submit, if --auto used
    submit_yes = True if args.auto else confirm("Also submit the job for you?")

    # if srf(variation) is provided as args, only create the slurm
    # with same name provided
    srf = params.srf_file[0]
    srf_name = os.path.splitext(os.path.basename(srf))[0]
    if args.srf is None or srf_name == args.srf:

        # WCT estimation
        if args.ascii:
            print("No time estimation available for ascii. "
                  "Using default WCT {}".format(default_wct))
            wct = default_wct
        else:
            # Use HF nt for wct estimation
            nt = int(float(params.sim_duration) / float(params.hf.hf_dt))
            fd_count = len(shared.get_stations(params.FD_STATLIST))

            est_core_hours, est_run_time = wc.estimate_BB_WC_single(
                fd_count, nt, ncores)
            wct = wc.get_wct(est_run_time)
            print("Estimated time: {} with {} number of cores".format(
                wc.convert_to_wct(est_run_time), ncores))

            print("Use the estimated wall clock time? (Minimum of 5 mins, "
                  "otherwise adds a 10% overestimation to ensure "
                  "the job completes)")
            use_estimation = show_yes_no_question()

            if use_estimation:
                wct = wc.get_wct(est_run_time)
            else:
                wct = str(install.get_input_wc())
            print("WCT set to: %s" % wct)

        bb_sim_dir = os.path.join(params.sim_dir, 'BB')
        # TODO: save status as HF. refer to submit_hf

        # Create/write the script
        script_file = write_sl_script(
            bb_sim_dir, params.sim_dir, srf_name, sl_name_prefix,
            nb_cpus=ncores, account=args.account, binary=not args.ascii,
            run_time=wct)

        # Submit the script
        submit_sl_script(script_file, "BB", 'queued', srf_name, submit_yes)
