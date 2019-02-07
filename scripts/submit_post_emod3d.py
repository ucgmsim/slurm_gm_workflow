#!/usr/bin/env python3
"""Script to create and submit a slurm script for LF"""
import os
import glob
import argparse
from datetime import datetime

from qcore import utils
from qcore import binary_version
from shared_workflow import shared
# TODO: remove this once temp_shared is gone
from temp_shared import resolve_header

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

merge_ts_name_prefix = "post_emod3d_merge_ts"
winbin_aio_name_prefix = "post_emod3d_winbin_aio"

# TODO: implement estimation for these numbers
default_run_time_merge_ts = "00:30:00"
default_run_time_winbin_aio = "02:00:00"
# default_core_merge_ts must be 4, higher number of cpu cause
# un-expected errors (TODO: maybe fix it when merg_ts's time become issue)
default_core_merge_ts = "4"
default_core_winbin_aio = "80"
default_memory = "16G"
default_account = 'nesi00213'

# TODO:the max number of cpu per node may need update when migrate machines
# this variable is critical to prevent crashes for winbin-aio
max_tasks_per_node = "80"


def get_seis_len(seis_path):
    filepattern = os.path.join(seis_path, '*_seis*.e3d')
    seis_file_list = sorted(glob.glob(filepattern))
    return len(seis_file_list)


def write_sl_script_merge_ts(
        lf_sim_dir, sim_dir, tools_dir, mgmt_db_location, rup_mod,
        run_time=default_run_time_merge_ts, nb_cpus=default_core_merge_ts,
        memory=default_memory, account=default_account):
    """Populates the template and writes the resulting slurm script to file"""
    with open('%s.sl.template' % merge_ts_name_prefix) as f:
        template = f.read()

    replace_t = [
        # TODO: the merge_ts binary needed to use relative path instead
        #  of absolute, maybe fix this
        ("{{lf_sim_dir}}", os.path.relpath(lf_sim_dir, sim_dir)),
        ("{{tools_dir}}", binary_version.get_unversioned_bin('merge_tsP3_par')),
        ("{{mgmt_db_location}}", mgmt_db_location),
        ("{{sim_dir}}", sim_dir),
        ("{{srf_name}}", rup_mod)
    ]

    for pattern, value in replace_t:
        template = template.replace(pattern, value)

    job_name = "post_emod3d.merge_ts.%s" % rup_mod
    header = resolve_header(
        account, nb_cpus, run_time, job_name, "Slurm", memory, timestamp,
        job_description="post emod3d: merge_ts",
        additional_lines="###SBATCH -C avx")

    script_name = '%s_%s_%s.sl' % (merge_ts_name_prefix, rup_mod, timestamp)
    with open(script_name, 'w') as f:
        f.write(header)
        f.write(template)

    script_name_abs = os.path.join(os.path.abspath(os.path.curdir),
                                   script_name)
    print("Slurm script %s written" % script_name)
    return script_name_abs


def write_sl_script_winbin_aio(
        lf_sim_dir, sim_dir, mgmt_db_location, rup_mod,
        run_time=default_run_time_winbin_aio, memory=default_memory,
        account=default_account):
    """Populates the template and writes the resulting slurm script to file"""
    # Read template
    with open('%s.sl.template' % winbin_aio_name_prefix) as f:
        template = f.read()

    # TODO: the merge_ts binrary needed to use relative path instead of
    #  absolute, maybe fix this
    template = template.replace(
        "{{lf_sim_dir}}", os.path.relpath(lf_sim_dir, sim_dir)).replace(
        "{{mgmt_db_location}}", mgmt_db_location).replace(
        "{{sim_dir}}", sim_dir).replace("{{srf_name}}", rup_mod)

    # Get the file count of seis files
    sfl_len = int(get_seis_len(
        os.path.join(os.path.join(sim_dir, lf_sim_dir), "OutBin")))

    # Round down to the max cpu per node
    nodes = int(round((sfl_len / int(max_tasks_per_node)) - 0.5))
    if nodes <= 0:
        # Use the same cpu count as the seis files
        nb_cpus = str(sfl_len)
    else:
        nb_cpus = str(nodes * int(max_tasks_per_node))

    job_name = "post_emod3d.winbin_aio.%s" % rup_mod
    header = resolve_header(
        account, nb_cpus, run_time, job_name, "slurm", memory, timestamp,
        job_description="post emod3d: winbin_aio", additional_lines="###SBATCH -C avx")

    script_name = '%s_%s_%s.sl' % (winbin_aio_name_prefix, rup_mod, timestamp)
    with open(script_name, 'w') as f:
        f.write(header)
        f.write(template)

    script_name_abs = os.path.join(os.path.abspath(os.path.curdir),
                                   script_name)
    print("Slurm script %s written" % script_name_abs)
    return script_name_abs


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Create (and submit if specified) the slurm script for HF")

    parser.add_argument("--auto", nargs="?", type=str, const=True)
    parser.add_argument('--account', type=str, default=default_account)
    parser.add_argument('--merge_ts', nargs="?", type=str, const=True)
    parser.add_argument('--winbin_aio', nargs="?", type=str, const=True)
    parser.add_argument('--srf', type=str, default=None)
    parser.add_argument('--pre_hf', nargs="?", type=str, const=True)

    args = parser.parse_args()

    created_scripts = []
    params = utils.load_sim_params('sim_params.yaml')
    submit_yes = True if args.auto else shared.confirm("Also submit the job for you?")
    # get the srf(rup) name without extensions
    srf_name = os.path.splitext(os.path.basename(params.srf_file))[0]
    # if srf(variation) is provided as args, only create the slurm
    # with same name provided
    if args.srf is None or srf_name == args.srf:
        # get lf_sim_dir
        lf_sim_dir = os.path.join(params.sim_dir, 'LF')

        # run merge_ts related scripts only
        # if non of args are provided, run script related to both
        if not args.winbin_aio and not args.merge_ts:
            args.merge_ts, args.winbin_aio = True, True

        if args.merge_ts:
            tools_dir = binary_version.get_lf_bin(params.emod3d.emod3d_version)
            script = write_sl_script_merge_ts(
                lf_sim_dir, params.sim_dir, tools_dir,
                params.mgmt_db_location, srf_name)
            shared.submit_sl_script(
                script, "merge_ts", "queued", params.mgmt_db_location,
                srf_name, timestamp, submit_yes=submit_yes)

        # run winbin_aio related scripts only
        if args.winbin_aio:
            script = write_sl_script_winbin_aio(
                lf_sim_dir, params.sim_dir, params.mgmt_db_location,
                srf_name)
            shared.submit_sl_script(
                script, "winbin_aio", "queued", params.mgmt_db_location,
                srf_name, timestamp, submit_yes=submit_yes)
