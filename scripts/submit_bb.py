#!/usr/bin/env python3
"""Script to create and submit a slurm script for BB"""
import os
import argparse

import qcore.constants as const
from estimation import estimate_wct as wc
from qcore import shared, utils
from qcore.config import host
from shared_workflow.load_config import load
from shared_workflow.shared import (
    set_wct,
    confirm,
    submit_sl_script,
    resolve_header,
    generate_context,
)

default_wct = "00:30:00"


def write_sl_script(
    bb_sim_dir,
    sim_dir,
    srf_name,
    params,
    sl_template_prefix,
    nb_cpus=const.BB_DEFAULT_NCORES,
    run_time=default_wct,
    memory=const.DEFAULT_MEMORY,
    account=const.DEFAULT_ACCOUNT,
    binary=False,
    machine=host,
):

    if binary:
        create_directory = "mkdir -p " + os.path.join(bb_sim_dir, "Acc") + "\n"
        submit_command = (
            create_directory + "srun python $gmsim/workflow/scripts/bb_sim.py "
        )
        arguments = [
            os.path.join(sim_dir, "LF", "OutBin"),
            params.vel_mod_dir,
            os.path.join(sim_dir, "HF", "Acc/HF.bin"),
            params.stat_vs_est,
            os.path.join(bb_sim_dir, "Acc/BB.bin"),
            "--flo",
            str(params.flo),
        ]
        additional_args = ["fmin", "fmidbot", "lfvsref"]
        for key in additional_args:
            if key in params.bb:
                arguments.append("--" + key)
                arguments.append(str(params.bb[key]))

        additional_flags = ["no-lf-amp"]
        for key in additional_flags:
            if key in params.bb:
                # seperated intentionally so the key will not be incerted when it is not there before.
                if params.bb[key] is True:
                    arguments.append("--" + key)
        bb_submit_command = submit_command + " ".join(arguments)
    else:
        bb_submit_command = (
            "srun python  $gmsim/workflow/scripts" "/match_seismo-mpi.py " + bb_sim_dir,
        )

    variation = srf_name.replace("/", "__")
    print(variation)

    test_bb_script = "test_bb_binary.sh" if binary else "test_bb_ascii.sh"

    template = generate_context(
        sim_dir,
        "%s.sl.template" % sl_template_prefix,
        rup_mod=variation,
        mgmt_db_location=params.mgmt_db_location,
        bb_submit_command=bb_submit_command,
        sim_dir=sim_dir,
        srf_name=srf_name,
        test_bb_script=test_bb_script,
    )
    print("sim dir, srf_name", sim_dir, srf_name)

    job_name = "sim_bb_%s" % variation
    header = resolve_header(
        account,
        str(nb_cpus),
        run_time,
        job_name,
        "slurm",
        memory,
        const.timestamp,
        job_description="BB calculation",
        additional_lines="##SBATCH -C avx",
        target_host=machine,
    )
    fname_sl_script = "%s_%s_%s.sl" % (sl_template_prefix, variation, const.timestamp)
    with open(fname_sl_script, "w") as f:
        f.write(header)
        f.write(template)

    fname_sl_abs_path = os.path.join(os.path.abspath(os.path.curdir), fname_sl_script)
    print("Slurm script %s written" % fname_sl_abs_path)
    return fname_sl_abs_path


def main(args):
    params = utils.load_sim_params("sim_params.yaml")

    ncores = const.BB_DEFAULT_NCORES
    if args.version is not None:
        version = args.version
        if version == "serial" or version == "run_bb":
            sl_name_prefix = "run_bb"
            ncores = 1
        elif version == "mp" or version == "run_bb_mp":
            sl_name_prefix = "run_bb_mp"
        elif version == "mpi" or version == "run_bb_mpi":
            sl_name_prefix = "run_bb_mpi"
        else:
            print("% cannot be recognize as a valide option" % version)
            print("version is set to default: %", const.BB_DEFAULT_VERSION)
            version = const.BB_DEFAULT_VERSION
            sl_name_prefix = const.BB_DEFAULT_VERSION
    else:
        version = const.BB_DEFAULT_VERSION
        sl_name_prefix = const.BB_DEFAULT_VERSION
    print(version)

    srf_name = os.path.splitext(os.path.basename(params.srf_file))[0]
    if args.srf is None or srf_name == args.srf:

        # WCT estimation
        if args.ascii:
            print(
                "No time estimation available for ascii. "
                "Using default WCT {}".format(default_wct)
            )
            wct = default_wct
        else:
            # Use HF nt for wct estimation
            nt = int(float(params.sim_duration) / float(params.hf.hf_dt))
            fd_count = len(shared.get_stations(params.FD_STATLIST))

            workflow_config = load(
                os.path.dirname(os.path.realpath(__file__)), "workflow_config.json"
            )

            est_core_hours, est_run_time = wc.est_BB_chours_single(
                fd_count,
                nt,
                ncores,
                os.path.join(workflow_config["estimation_models_dir"], "BB"),
            )
            wct = set_wct(est_run_time, ncores, args.auto)

        bb_sim_dir = os.path.join(params.sim_dir, "BB")
        # TODO: save status as HF. refer to submit_hf

        # Create/write the script
        script_file = write_sl_script(
            bb_sim_dir,
            params.sim_dir,
            srf_name,
            params=params,
            sl_template_prefix=sl_name_prefix,
            nb_cpus=ncores,
            account=args.account,
            binary=not args.ascii,
            run_time=wct,
            machine=args.machine,
        )

        # Submit the script
        submit_yes = True if args.auto else confirm("Also submit the job for you?")
        submit_sl_script(
            script_file,
            "BB",
            "queued",
            params.mgmt_db_location,
            srf_name,
            const.timestamp,
            submit_yes=submit_yes,
            target_machine=args.machine,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create (and submit if specified) the slurm script for BB"
    )

    parser.add_argument("--auto", nargs="?", type=str, const=True)
    parser.add_argument("--version", type=str, default=const.BB_DEFAULT_VERSION)
    parser.add_argument("--account", type=str, default=const.DEFAULT_ACCOUNT)
    parser.add_argument("--srf", type=str, default=None)
    parser.add_argument("--ascii", action="store_true", default=False)
    parser.add_argument(
        "--machine",
        type=str,
        default=host,
        help="The machine bb is to be submitted to.",
    )
    args = parser.parse_args()

    main(args)
