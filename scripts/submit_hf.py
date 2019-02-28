#!/usr/bin/env python3
"""Script to create and submit a slurm script for HF"""
import os
import os.path
import argparse

from jinja2 import Environment, FileSystemLoader

import estimation.estimate_wct as est
import qcore.constants as const
from qcore import utils, shared, srf, binary_version
from qcore.config import get_machine_config, host
from shared_workflow.shared import confirm, set_wct, submit_sl_script, resolve_header

# default values
default_wct = "00:30:00"


def generate_context(
    template_path,
    hf_sim_dir,
    mgmt_db_location,
    hf_submit_command,
    sim_dir,
    stoch_name,
    binary,
):
    test_hf_script = "test_hf_binary.sh" if binary else "test_hf_ascii.sh"
    j2_env = Environment(loader=FileSystemLoader(sim_dir), trim_blocks=True)
    context = j2_env.get_template(template_path).render(
        hf_sim_dir=hf_sim_dir,
        hf_submit_command=hf_submit_command,
        mgmt_db_location=mgmt_db_location,
        sim_dir=sim_dir,
        srf_name=stoch_name,
        test_hf_script=test_hf_script,
    )
    return context


def write_sl_script(
    hf_sim_dir,
    sim_dir,
    stoch_name,
    sl_template_prefix,
    hf_option,
    params,
    nb_cpus=const.HF_DEFAULT_NCORES,
    wct=default_wct,
    memory=const.DEFAULT_MEMORY,
    account=const.DEFAULT_ACCOUNT,
    binary=False,
    seed=None,
    machine=host,
):
    """Populates the template and writes the resulting slurm script to file"""

    target_qconfig = get_machine_config(machine)

    if binary:
        create_dir = "mkdir -p " + os.path.join(hf_sim_dir, "Acc") + "\n"
        hf_submit_command = (
            create_dir + "srun python $gmsim/workflow" "/scripts/hf_sim.py "
        )
        arguments_for_hf = [
            params.hf.hf_slip,
            params.FD_STATLIST,
            os.path.join(hf_sim_dir, "Acc/HF.bin"),
            "-m",
            params.v_mod_1d_name,
            "--duration",
            params.sim_duration,
            "--dt",
            params.hf.hf_dt,
            "--sim_bin",
            binary_version.get_hf_binmod(
                params.hf.hf_version, target_qconfig["tools_dir"]
            ),
        ]
        additional_args = ["hf_path_dur"]
        for key in additional_args:
            if key in params.hf:
                arguments_for_hf.append("--" + key)
                arguments_for_hf.append(str(params.hf[key]))
        hf_submit_command += " ".join(list(map(str, arguments_for_hf)))
    else:
        hf_submit_command = (
            "srun python  $gmsim/workflow/scripts"
            "/hfsims-stats-mpi.py " + hf_sim_dir + " " + str(hf_option)
        )

    if seed is not None:
        hf_submit_command = "{} --seed {}".format(hf_submit_command, seed)

    # Replace template values
    template = generate_context(
        "%s.sl.template" % sl_template_prefix,
        hf_sim_dir,
        params.mgmt_db_location,
        hf_submit_command,
        sim_dir,
        stoch_name,
        binary,
    )

    variation = stoch_name.replace("/", "__")
    print(variation)

    job_name = "sim_hf.%s" % variation
    header = resolve_header(
        account,
        str(nb_cpus),
        wct,
        job_name,
        "slurm",
        memory,
        const.timestamp,
        job_description="HF calculation",
        additional_lines="###SBATCH -C avx",
        target_host=machine,
    )
    script_name = "%s_%s_%s.sl" % (sl_template_prefix, variation, const.timestamp)
    with open(script_name, "w") as f:
        f.write(header)
        f.write(template)

    script_name_abs = os.path.join(os.path.abspath(os.path.curdir), script_name)
    print("Slurm script %s written" % script_name_abs)
    return script_name_abs


def main(args):
    params = utils.load_sim_params("sim_params.yaml")

    # check if the args is none, if not, change the version
    ncore = args.ncore
    if args.version is not None:
        version = args.version
        if version == "serial" or version == "run_hf":
            ll_name_prefix = "run_hf"
            ncore = 1
        if version == "mp" or version == "run_hf_mp":
            wl_name_prefix = "run_hf_mp"
        elif version == "mpi" or version == "run_hf_mpi":
            ll_name_prefix = "run_hf_mpi"
        else:
            print("% cannot be recognize as a valide option" % version)
            print("version is set to default: %", const.HF_DEFAULT_VERSION)
            version = const.HF_DEFAULT_VERSION
            ll_name_prefix = const.HF_DEFAULT_VERSION
    else:
        version = const.HF_DEFAULT_VERSION
        ll_name_prefix = const.HF_DEFAULT_VERSION
    print("version:", version)

    # check rand_reset
    if args.site_specific is not None or params.bb.site_specific:
        print("Note: site_specific = True, rand_reset = True")
        hf_option = 2
    else:
        try:
            if args.rand_reset is not None or params.bb.rand_reset:
                hf_option = 1
            else:
                hf_option = 0
        except:
            hf_option = 0
            print(
                "Note: rand_reset is not defined in params_base_bb.py. "
                "We assume rand_reset=%s" % bool(hf_option)
            )

    # modify the logic to use the same as in install_bb:
    # sniff through params_base to get the names of srf,
    # instead of running through file directories.

    # loop through all srf file to generate related slurm scripts
    srf_name = os.path.splitext(os.path.basename(params.srf_file))[0]
    # if srf(variation) is provided as args, only create
    # the slurm with same name provided
    if args.srf is None or srf_name == args.srf:
        nt = int(float(params.sim_duration) / float(params.hf.hf_dt))
        fd_count = len(shared.get_stations(params.FD_STATLIST))
        # TODO:make it read through the whole list
        #  instead of assuming every stoch has same size
        nsub_stoch, sub_fault_area = srf.get_nsub_stoch(
            params.hf.hf_slip, get_area=True
        )

        if args.debug:
            print("sb:", sub_fault_area)
            print("nt:", nt)
            print("fd:", fd_count)
            print("nsub:", nsub_stoch)

        if args.ascii:
            print(
                "No time estimation available for ascii. "
                "Using default WCT {}".format(default_wct)
            )
            wct = default_wct
        else:
            est_core_hours, est_run_time = est.est_HF_chours_single(
                fd_count, nsub_stoch, nt, ncore
            )
            wct = set_wct(est_run_time, ncore, args.auto)

        hf_sim_dir = os.path.join(params.sim_dir, "HF")

        # Create/write the script
        script_file = write_sl_script(
            hf_sim_dir=hf_sim_dir,
            sim_dir=params.sim_dir,
            stoch_name=srf_name,
            sl_template_prefix=ll_name_prefix,
            hf_option=hf_option,
            params=params,
            nb_cpus=ncore,
            wct=wct,
            account=args.account,
            binary=not args.ascii,
            seed=args.seed,
            machine=args.machine,
        )

        # Submit the script
        submit_yes = True if args.auto else confirm("Also submit the job for you?")
        submit_sl_script(
            script_file,
            "HF",
            "queued",
            params.mgmt_db_location,
            srf_name,
            const.timestamp,
            submit_yes=submit_yes,
            target_machine=args.machine,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create (and submit if specified) the slurm script for HF"
    )

    parser.add_argument("--version", type=str, default=None, const=None)
    parser.add_argument("--ncore", type=int, default=const.HF_DEFAULT_NCORES)

    # if the --auto flag is used, wall clock time will be estimated the job
    # submitted automatically
    parser.add_argument("--auto", type=int, nargs="?", default=None, const=True)

    # rand_reset, if somehow the user decide to use it but not defined in
    # params_base_bb the const is set to True, so that as long as --rand_reset
    # is used, no more value needs to be provided
    parser.add_argument("--rand_reset", type=int, nargs="?", default=None, const=True)

    parser.add_argument(
        "--site_specific", type=int, nargs="?", default=None, const=True
    )
    parser.add_argument("--account", type=str, default=const.DEFAULT_ACCOUNT)
    parser.add_argument("--srf", type=str, default=None)
    parser.add_argument("--ascii", action="store_true", default=False)
    parser.add_argument("--debug", action="store_true", default=False)
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="random seed number(0 for randomized seed)",
    )
    parser.add_argument(
        "--machine",
        type=str,
        default=host,
        help="The machine hf is to be submitted to.",
    )
    args = parser.parse_args()

    main(args)
