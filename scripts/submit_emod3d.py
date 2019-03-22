#!/usr/bin/env python3
"""Script to create and submit a slurm script for LF"""
# TODO: import the CONFIG here
# Section for parser to determine if using automate wct
import os
import argparse

import scripts.set_runparams as set_runparams
import qcore.constants as const
import estimation.estimate_wct as est
from qcore import utils, binary_version
from qcore.config import get_machine_config, host
from shared_workflow import load_config
from shared_workflow.shared import (
    confirm,
    set_wct,
    submit_sl_script,
    resolve_header,
    get_nt,
    generate_context,
)

# Estimated number of minutes between each checkpoint
CHECKPOINT_DURATION = 10


def write_sl_script(
    lf_sim_dir,
    sim_dir,
    srf_name,
    mgmt_db_location,
    binary_path,
    run_time="02:00:00",
    nb_cpus=const.LF_DEFAULT_NCORES,
    memory=const.DEFAULT_MEMORY,
    account=const.DEFAULT_ACCOUNT,
    machine=host,
    steps_per_checkpoint=None,
    write_directory=".",
):
    """Populates the template and writes the resulting slurm script to file"""
    workflow_config = load_config.load(
        os.path.dirname(os.path.realpath(__file__)), "workflow_config.json"
    )

    set_runparams.create_run_params(
        srf_name,
        workflow_config=workflow_config,
        steps_per_checkpoint=steps_per_checkpoint,
    )

    template = generate_context(
        sim_dir,
        "run_emod3d.sl.template",
        lf_sim_dir=lf_sim_dir,
        tools_dir=binary_path,
        mgmt_db_location=mgmt_db_location,
        sim_dir=sim_dir,
        srf_name=srf_name,
    )

    # slurm header
    job_name = "run_emod3d.%s" % srf_name
    header = resolve_header(
        account,
        str(nb_cpus),
        run_time,
        job_name,
        "slurm",
        memory,
        const.timestamp,
        job_description="emod3d slurm script",
        additional_lines="#SBATCH --hint=nomultithread",
        target_host=machine,
        write_directory=write_directory,
    )

    fname_slurm_script = os.path.abspath(
        os.path.join(
            write_directory, "run_emod3d_{}_{}.sl".format(srf_name, const.timestamp)
        )
    )
    with open(fname_slurm_script, "w") as f:
        f.write(header)
        f.write("\n")
        f.write(template)

    print("Slurm script %s written" % fname_slurm_script)

    return fname_slurm_script


def main(args):
    params = utils.load_sim_params(os.path.join(args.rel_dir, "sim_params.yaml"))

    submit_yes = True if args.auto else confirm("Also submit the job for you?")

    print("params.srf_file", params.srf_file)
    # Get the srf(rup) name without extensions
    srf_name = os.path.splitext(os.path.basename(params.srf_file))[0]
    if args.srf is None or srf_name == args.srf:
        print("not set_params_only")
        # get lf_sim_dir
        lf_sim_dir = os.path.join(params.sim_dir, "LF")
        sim_dir = params.sim_dir

        # default_core will be changed is user passes ncore
        n_cores = args.ncore
        est_core_hours, est_run_time, n_cores = est.est_LF_chours_single(
            int(params.nx),
            int(params.ny),
            int(params.nz),
            get_nt(params),
            n_cores,
            True,
        )
        wct = set_wct(est_run_time, n_cores, args.auto)

        target_qconfig = get_machine_config(args.machine)

        binary_path = binary_version.get_lf_bin(
            params.emod3d.emod3d_version, target_qconfig["tools_dir"]
        )
        steps_per_checkpoint = int(
            get_nt(params) / (60.0 * est_run_time) * CHECKPOINT_DURATION
        )

        script = write_sl_script(
            lf_sim_dir,
            sim_dir,
            srf_name,
            params.mgmt_db_location,
            binary_path,
            run_time=wct,
            nb_cpus=n_cores,
            machine=args.machine,
            steps_per_checkpoint=steps_per_checkpoint,
        )

        submit_sl_script(
            script,
            "EMOD3D",
            "queued",
            params.mgmt_db_location,
            srf_name,
            const.timestamp,
            submit_yes=submit_yes,
            target_machine=args.machine,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create (and submit if specified) the slurm script for LF"
    )

    parser.add_argument("--ncore", type=int, default=const.LF_DEFAULT_NCORES)
    parser.add_argument("--auto", nargs="?", type=str, const=True)
    parser.add_argument("--account", type=str, default=const.DEFAULT_ACCOUNT)
    parser.add_argument("--srf", type=str, default=None)
    parser.add_argument(
        "--machine",
        type=str,
        default=host,
        help="The machine emod3d is to be submitted to.",
    )
    parser.add_argument(
        "--write_directory",
        type=str,
        help="The directory to write the slurm script to.",
        default=".",
    )
    parser.add_argument(
        "--rel_dir", default=".", type=str, help="The path to the realisation directory"
    )
    args = parser.parse_args()

    main(args)
