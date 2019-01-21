"""
 python submit_imcalc.py
    -obs ~/test_obs/IMCalcExample/
    -sim runs/Runs
    -srf /nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p6_batched/
        v18p6_exclude_1k_batch_6/Data/Sources
    -ll /scale_akl_nobackup/filesets/transit/nesi00213/StationInfo/
        non_uniform_whole_nz_with_real_stations-hh400_v18p6.ll
    -o ~/rrup_out
    -ml 1000 -e -s -i OtaraWest02_HYP01-21_S1244 Pahiatua_HYP01-26_S1244
    -t 24:00:00
"""
import argparse
import os
import glob
import time
import getpass
import re
from time import sleep
from datetime import datetime

from jinja2 import Template, Environment, FileSystemLoader

import im_calc_checkpoint as checkpoint
from qcore import utils, shared
from estimation.estimate_WC import get_IM_comp_count, est_IM_chours_single
from shared_workflow.shared import exe, submit_sl_script, update_db_cmd, set_wct

timestamp_format = "%Y%m%d_%H%M%S"
timestamp = datetime.now().strftime(timestamp_format)

TEMPLATES_DIR = "templates"
CONTEXT_TEMPLATE = "im_calc.sl.template"
HEADER_TEMPLATE = "slurm_header.cfg"
SL_NAME = "im_calc_{}_{{timestamp}}_{}.sl".replace("{{timestamp}}", timestamp)
SKIP = "skip"
TIME = "00:30:00"
DEFAULT_N_PROCESSES = 40
DEFAULT_RRUP_OUTDIR = os.path.join(
    "/home",
    getpass.getuser(),
    "imcalc_rrup_out_{}".format(
        datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d-%H-%M-%S")
    ),
)
PARAMS_BASE = "params_base.py"
VERSION = "slurm"
JOB = "im_calc"
ACCOUNT = "nesi00213"
NTASKS = 1
EXE_TIME = "%j"
MAIL = "test@test.com"
MEMORY = "2G"
ADDI = "#SBATCH --hint=nomultithread"
ADDI = ADDI + "\n" + "#SBATCH --mem-per-cpu=%s" % MEMORY
TIME_REGEX = "(24:00:00)|(^(2[0-3]|[01]?[0-9]):([0-5]?[0-9]):([0-5]?[0-9])$)"
COMPS = ["geom", "000", "090", "ver", "ellipsis"]

# TODO: calculate wall-clock time
# TODO: read fd*.ll file to limit the stations that rrups is calculated for
# TODO: option for binary workflow
# TODO: handle optional arguments correctly
# TODO: rrup output_dir the csv to each individual simulation folder
# TODO: one rupture distance calc per fault
# TODO: remove relative paths on sl.template


def write_sl(name_context_list, submit=False, mgmt_db_loc=None):
    """Writes the slrum script to file and submits if specifed"""
    for sl_name, context, srf_list in name_context_list:
        with open(sl_name, "w") as sl:
            print("writing {}".format(sl_name))
            sl.write(context)
        if submit is True:
            jobid = submit_sl_script(
                os.path.realpath(sl_name),
                "IM_calculation",
                "queued",
                mgmt_db_loc,
                srf_list[0],
                timestamp,
            )

            if jobid is not None and mgmt_db_loc is not None:
                for srf_name in srf_list[1:]:
                    update_db_cmd(
                        "IM_calculation",
                        "queued",
                        mgmt_db_loc,
                        srf_name,
                        jobid,
                        timestamp,
                    )


def get_basename_without_ext(path):
    return os.path.splitext(os.path.basename(path))[0]


def get_fault_name(run_name):
    return run_name.split("_")[0]


def split_and_generate_slurms(
    sim_dirs,
    obs_dirs,
    station_file,
    rrup_files,
    output_dir,
    n_procs,
    max_lines,
    prefix,
    extended="",
    simple="",
    comp=COMPS[0],
    version=VERSION,
    job_description="",
    job_name=JOB,
    account=ACCOUNT,
    n_tasks=NTASKS,
    wallclock_limit=TIME,
    exe_time=EXE_TIME,
    mail=MAIL,
    memory=MEMORY,
    additional_lines=ADDI,
    mgmt_db=None,
):
    total_dir_lines = 0
    if sim_dirs != []:
        total_dir_lines = len(sim_dirs)
    elif obs_dirs != []:
        total_dir_lines = len(obs_dirs)
    elif rrup_files != []:
        total_dir_lines = len(rrup_files)

    name_context_list = []
    i = 0
    while i < total_dir_lines:
        last_line_index = i + max_lines
        if 0 <= last_line_index - total_dir_lines <= max_lines:
            last_line_index = total_dir_lines

        template_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", TEMPLATES_DIR
        )

        # Header
        j2_env = Environment(loader=FileSystemLoader(template_path), trim_blocks=True)
        header = j2_env.get_template(HEADER_TEMPLATE).render(
            version=version,
            job_description=job_description,
            job_name=job_name,
            account=account,
            n_tasks=n_tasks,
            wallclock_limit=wallclock_limit,
            exe_time=exe_time,
            mail=mail,
            memory=memory,
            additional_lines=additional_lines,
        )

        # Context
        j2_env = Environment(loader=FileSystemLoader(template_path), trim_blocks=True)
        context = j2_env.get_template(CONTEXT_TEMPLATE).render(
            time=time,
            comp=comp,
            sim_dirs=sim_dirs,
            obs_dirs=obs_dirs,
            rrup_files=rrup_files,
            station_file=station_file,
            output_dir=output_dir,
            np=n_procs,
            extended=extended,
            simple=simple,
            mgmt_db_location=mgmt_db,
        )

        name = SL_NAME.format(prefix, i)
        context = "{}\n{}".format(header, context)

        # append the name of sl, content of sl and related srf names
        if not sim_dirs:
            srf_name = None
        else:
            srf_name = list(zip(*sim_dirs))[1][i:last_line_index]
        name_context_list.append((name, context, srf_name))
        i += max_lines

    return name_context_list


def get_fd_path(srf_filepath, sim_dir):
    fd = ""
    run_name = get_fault_name(get_basename_without_ext(srf_filepath))
    if sim_dir is not None:
        fault_dir = os.path.join(sim_dir, run_name)
        try:
            fd_path = utils.load_yaml(os.path.join(fault_dir, "sim_params.yaml"))[
                "FD_STATLIST"
            ]
            fd = "-fd {}".format(fd_path)
        except Exception as e:
            fd = SKIP
    return fd


def get_dirs(run_folder, arg_identifiers, com_pattern):
    dirs = []
    for identifier in arg_identifiers:
        fault_name = get_fault_name(identifier)
        dir_path = os.path.join(run_folder, com_pattern.format(fault_name, identifier))
        if glob.glob(dir_path) == []:
            print("{} does not exists".format(dir_path))
        else:
            dirs.append(dir_path)
    return dirs


def main():
    parser = argparse.ArgumentParser(
        description="Prints out a slurm script to run IM Calculation over a run-group"
    )
    parser.add_argument(
        "-sim",
        "--sim_dir",
        help="Path to sim-run-group containing faults and acceleration in the subfolder */BB/*/*",
    )
    parser.add_argument(
        "-obs",
        "--obs_dir",
        help="Path to obs-run-group containing faults and accelerations in the subfolder */*/accBB",
    )
    parser.add_argument(
        "-srf",
        "--srf_dir",
        help="Path to run-group containing the srf files in the path matching */Srf/*.srf",
    )
    parser.add_argument(
        "-ll",
        "--station_file",
        help="Path to a single station file for ruputure distance calculations",
    )
    parser.add_argument(
        "-np",
        "--processes",
        default=DEFAULT_N_PROCESSES,
        help="number of processors to use",
    )
    parser.add_argument(
        "-ml",
        "--max_lines",
        default=100,
        type=int,
        help="maximum number of lines in a slurm script. Default 100",
    )
    parser.add_argument(
        "-e",
        "--extended_period",
        action="store_const",
        const="-e",
        default="",
        help="add '-e' to indicate the use of extended pSA period. Default not using",
    )
    parser.add_argument(
        "-s",
        "--simple_output",
        action="store_const",
        const="-s",
        default="",
        help="Please add '-s' to indicate if you want to output the big summary csv "
        "only(no single station csvs). Default outputting both single station and "
        "the big summary csvs",
    )
    parser.add_argument(
        "-o",
        "--rrup_out_dir",
        default=DEFAULT_RRUP_OUTDIR,
        help="output directory to store rupture distances output.Default is {}".format(
            DEFAULT_RRUP_OUTDIR
        ),
    )
    parser.add_argument(
        "-c",
        "--comp",
        default=COMPS[0],
        help="specify which verlocity compoent to calculate. choose from {}. Default is {}".format(
            COMPS, COMPS[0]
        ),
    )
    parser.add_argument(
        "-i",
        "--identifiers",
        default="*",
        nargs="+",
        help="a list of space-seperated unique runnames of the simulations."
        "eg.'Albury_HYP01-01_S1244 OpouaweUruti_HYP44-47_S1674'",
    )
    parser.add_argument(
        "-t",
        "--time",
        default=TIME,
        help="estimated running time for each slurm sciprt. must be in 'hh:mm:ss' format. "
        "Default is {}".format(TIME),
    )
    parser.add_argument("--version", default=VERSION, help="default version is 'slurm'")
    parser.add_argument(
        "--job_description",
        default=JOB,
        help="job description of slurm script. Default is {}".format(JOB),
    )
    parser.add_argument(
        "--job_name",
        default=JOB,
        help="job name of slurm script. Default is {}".format(JOB),
    )
    parser.add_argument(
        "--account",
        default=ACCOUNT,
        help="Account name. Default is {}.".format(ACCOUNT),
    )
    parser.add_argument(
        "--ntasks",
        default=NTASKS,
        type=int,
        help="number of tasks per node. Default is {}".format(NTASKS),
    )
    parser.add_argument(
        "--exe_time",
        default=EXE_TIME,
        help="Default is {}".format(EXE_TIME.replace("%", "%%")),
    )
    parser.add_argument("--mail", default=MAIL, help="Default is {}".format(MAIL))
    parser.add_argument(
        "--memory", default=MEMORY, help="Memory per cpu. Default is {}".format(MEMORY)
    )
    parser.add_argument(
        "--additional_lines",
        default=ADDI,
        help="additional lines add to slurm header. Default is {}".format(ADDI),
    )
    parser.add_argument("--mgmt_db", type=str, help="path to the mgmt_db")

    parser.add_argument(
        "--auto", action="store_true", help="submit the sl script as well"
    )

    args = parser.parse_args()

    # Load the yaml params
    params = utils.load_sim_params("sim_params.yaml")

    if args.srf_dir is not None:
        utils.setup_dir(args.rrup_out_dir)

    if args.max_lines <= 0:
        parser.error(
            "-ml argument should come with a number that is "
            "0 < -ml <= (max_lines-header_and_other_prints) allowed by slurm"
        )

    if not re.match(TIME_REGEX, args.time):
        parser.error("time must be in 'hh:mm:ss' format and not exceeding 24:00:00")

    if not args.comp in COMPS:
        parser.error(
            "verlocity component must be in {} where ellipsis means calculating "
            "all compoents".format(COMPS)
        )

    if args.sim_dir is not None:
        sim_waveform_dirs = get_dirs(args.sim_dir, args.identifiers, "{}/{}/BB")
        sim_waveform_dirs = checkpoint.checkpoint_wrapper(
            args.sim_dir, sim_waveform_dirs, "s"
        )
        sim_run_names = list(map(os.path.basename, map(os.path.dirname, sim_waveform_dirs)))
        sim_faults = list(map(get_fault_name, sim_run_names))
        sim_dirs = list(zip(sim_waveform_dirs, sim_run_names, sim_faults))

        # Does not overwrite user-specified time
        wct = args.time
        if args.auto or args.time == TIME:
            print("Running wall clock estimation for IM sim")
            est_core_hours, est_run_time = est_IM_chours_single(
                len(shared.get_stations(params.FD_STATLIST)),
                int(float(params.sim_duration) / float(params.hf.hf_dt)),
                [args.comp],
                100 if args.extended_period else 15,
                args.processes,
            )
            wct = set_wct(est_run_time, args.processes, args.auto)

        # sim
        name_context_list = split_and_generate_slurms(
            sim_dirs,
            [],
            args.station_file,
            [],
            args.rrup_out_dir,
            args.processes,
            args.max_lines,
            "sim",
            extended=args.extended_period,
            simple=args.simple_output,
            comp=args.comp,
            version=args.version,
            job_description=args.job_description,
            job_name=args.job_name,
            account=args.account,
            n_tasks=args.ntasks,
            wallclock_limit=wct,
            exe_time=args.exe_time,
            mail=args.mail,
            memory=args.memory,
            additional_lines=args.additional_lines,
            mgmt_db=args.mgmt_db,
        )
        write_sl(name_context_list, args.auto, args.mgmt_db)

    if args.srf_dir is not None:
        srf_files = get_dirs(args.srf_dir, args.identifiers, "{}/Srf/{}.srf")
        # srf_files = glob.glob(os.path.join(args.srf_dir, "*/Srf/*.srf"))
        srf_files = checkpoint.checkpoint_rrup(args.rrup_out_dir, srf_files)
        rrup_files = []
        for srf_file in srf_files:
            fd = get_fd_path(srf_file, args.sim_dir)
            if fd != SKIP:
                run_name = get_basename_without_ext(srf_file)
                rrup_files.append((srf_file, run_name, fd))
        # rrup
        name_context_list = split_and_generate_slurms(
            [],
            [],
            args.station_file,
            rrup_files,
            args.rrup_out_dir,
            args.processes,
            args.max_lines,
            "rrup",
            version=args.version,
            job_description=args.job_description,
            job_name=args.job_name,
            account=args.account,
            n_tasks=args.ntasks,
            wallclock_limit=args.time,
            exe_time=args.exe_time,
            mail=args.mail,
            memory=args.memory,
            additional_lines=args.additional_lines,
        )
        write_sl(name_context_list)

    if args.obs_dir is not None:
        obs_waveform_dirs = glob.glob(os.path.join(args.obs_dir, "*"))
        obs_waveform_dirs = checkpoint.checkpoint_wrapper(
            args.obs_dir, obs_waveform_dirs, "o"
        )
        obs_run_names = list(map(os.path.basename, obs_waveform_dirs))
        obs_faults = list(map(get_fault_name, obs_run_names))
        obs_dirs = list(zip(obs_waveform_dirs, obs_run_names, obs_faults))
        # obs
        name_context_list = split_and_generate_slurms(
            [],
            obs_dirs,
            args.station_file,
            [],
            args.rrup_out_dir,
            args.processes,
            args.max_lines,
            "obs",
            extended=args.extended_period,
            simple=args.simple_output,
            comp=args.comp,
            version=args.version,
            job_description=args.job_description,
            job_name=args.job_name,
            account=args.account,
            n_tasks=args.ntasks,
            wallclock_limit=args.time,
            exe_time=args.exe_time,
            mail=args.mail,
            memory=args.memory,
            additional_lines=args.additional_lines,
        )
        write_sl(name_context_list)


if __name__ == "__main__":
    main()
