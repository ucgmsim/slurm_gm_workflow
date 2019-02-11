#!/usr/bin/env python3
"""Script to create and submit a slurm script for LF"""
# TODO: import the CONFIG here
# Section for parser to determine if using automate wct
import os
import argparse


import set_runparams
from qcore import utils, binary_version
from qcore.config import get_machine_config
import estimation.estimate_wct as wc
from shared_workflow import load_config
from shared_workflow.shared import confirm, set_wct, submit_sl_script, resolve_header


from datetime import datetime

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

# Default values
default_core = 160
default_run_time = "02:00:00"
default_memory = "16G"
default_account = "nesi00213"


def write_sl_script(
    lf_sim_dir,
    sim_dir,
    srf_name,
    mgmt_db_location,
    run_time=default_run_time,
    nb_cpus=default_core,
    memory=default_memory,
    account=default_account,
    machine="maui",
):
    """Populates the template and writes the resulting slurm script to file"""
    workflow_config = load_config.load(
        os.path.dirname(os.path.realpath(__file__)), "workflow_config.json"
    )

    set_runparams.create_run_params(srf_name, workflow_config=workflow_config)

    target_qconfig = get_machine_config(machine)

    binary_path = binary_version.get_lf_bin(
        params.emod3d.emod3d_version, target_qconfig["tools_dir"]
    )

    with open("run_emod3d.sl.template", "r") as f:
        template = f.read()

    replace_t = [
        ("{{lf_sim_dir}}", lf_sim_dir),
        ("{{tools_dir}}", binary_path),
        ("{{mgmt_db_location}}", mgmt_db_location),
        ("{{sim_dir}}", sim_dir),
        ("{{srf_name}}", srf_name),
    ]

    for pattern, value in replace_t:
        template = template.replace(pattern, value)

    # slurm header
    job_name = "run_emod3d.%s" % srf_name
    header = resolve_header(
        account,
        str(nb_cpus),
        run_time,
        job_name,
        "slurm",
        memory,
        timestamp,
        job_description="emod3d slurm script",
        additional_lines="#SBATCH --hint=nomultithread",
    )

    fname_slurm_script = "run_emod3d_%s_%s.sl" % (srf_name, timestamp)
    with open(fname_slurm_script, "w") as f:
        f.write(header)
        f.write(template)

    fname_sl_abs_path = os.path.join(
        os.path.abspath(os.path.curdir), fname_slurm_script
    )
    print("Slurm script %s written" % fname_sl_abs_path)

    return fname_sl_abs_path


if __name__ == "__main__":
    # Start of main function
    parser = argparse.ArgumentParser(
        description="Create (and submit if specified) the slurm script for LF"
    )

    parser.add_argument("--ncore", type=int, default=default_core)
    parser.add_argument("--auto", nargs="?", type=str, const=True)
    parser.add_argument("--account", type=str, default=default_account)
    parser.add_argument("--srf", type=str, default=None)
    args = parser.parse_args()

    params = utils.load_sim_params("sim_params.yaml")
    submit_yes = True if args.auto else confirm("Also submit the job for you?")

    print("params.srf_file", params.srf_file)
    wall_clock_limit = None
    # Get the srf(rup) name without extensions
    srf_name = os.path.splitext(os.path.basename(params.srf_file))[0]
    if args.srf is None or srf_name == args.srf:
        print("not set_params_only")
        # get lf_sim_dir
        lf_sim_dir = os.path.join(params.sim_dir, "LF")
        sim_dir = params.sim_dir

        # default_core will be changed is user passes ncore
        n_cores = args.ncore
        est_core_hours, est_run_time, n_cores = wc.est_LF_chours_single(
            int(params.nx),
            int(params.ny),
            int(params.nz),
            int(float(params.sim_duration) / float(params.dt)),
            n_cores,
            True,
        )
        wc = set_wct(est_run_time, n_cores, args.auto)

        script = write_sl_script(
            lf_sim_dir,
            sim_dir,
            srf_name,
            params.mgmt_db_location,
            run_time=wc,
            nb_cpus=n_cores,
            machine=args.machine,
        )

        submit_sl_script(
            script,
            "EMOD3D",
            "queued",
            params.mgmt_db_location,
            srf_name,
            timestamp,
            submit_yes=submit_yes,
            target_machine=args.machine,
        )
