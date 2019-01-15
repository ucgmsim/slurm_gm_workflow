"""
 python submit_imcalc.py -obs ~/test_obs/IMCalcExample/ -sim runs/Runs -srf /nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p6_batched/v18p6_exclude_1k_batch_6/Data/Sources -ll /scale_akl_nobackup/filesets/transit/nesi00213/StationInfo/non_uniform_whole_nz_with_real_stations-hh400_v18p6.ll -o ~/rrup_out -ml 1000 -e -s -i OtaraWest02_HYP01-21_S1244 Pahiatua_HYP01-26_S1244 -t 24:00:00
"""

from jinja2 import Template, Environment, FileSystemLoader
import argparse
import os
import glob
import getpass
import time
from time import sleep
from datetime import datetime
import im_calc_checkpoint as checkpoint
from qcore import utils
import re

from management import db_helper
from management import update_mgmt_db

from shared_workflow.shared import exe

timestamp_format = "%Y%m%d_%H%M%S"
timestamp = datetime.now().strftime(timestamp_format)

TEMPLATES_DIR = 'templates'
CONTEXT_TEMPLATE = 'im_calc.sl.template'
HEADER_TEMPLATE = 'slurm_header.cfg'
SL_NAME = 'im_calc_{}_{{timestamp}}_{}.sl'.replace('{{timestamp}}',timestamp)
SKIP = 'skip'
TIME = '00:30:00'
DEFAULT_N_PROCESSES = 40
DEFAULT_RRUP_OUTDIR = os.path.join('/home', getpass.getuser(), 'imcalc_rrup_out_{}'.format(
    datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H-%M-%S')))
VERSION = 'slurm'
JOB = 'im_calc'
ACCOUNT = 'nesi00213'
NTASKS = 1
EXE_TIME = '%j'
MAIL = 'test@test.com'
MEMORY = '2G'
ADDI = '#SBATCH --hint=nomultithread'
ADDI = ADDI+'\n'+'#SBATCH --mem-per-cpu=%s'%MEMORY
TIME_REGEX = '(24:00:00)|(^(2[0-3]|[01]?[0-9]):([0-5]?[0-9]):([0-5]?[0-9])$)'
COMPS=['geom', '000', '090', 'ver', 'ellipsis']

# TODO: calculate wall-clock time
# TODO: read fd*.ll file to limit the stations that rrups is calculated for
# TODO: option for binary workflow
# TODO: handle optional arguments correctly
# TODO: rrup output_dir the csv to each individual simulation folder
# TODO: one rupture distance calc per fault
# TODO: remove relative paths on sl.template
# python generate_sl.py -o ~/test_obs/IMCalcExample  -ll /scale_akl_nobackup/filesets/transit/nesi00213/StationInfo/non_uniform_whole_nz_with_real_stations-hh400_v18p6.ll -ml 1000 -simple -e


def generate_header(template_dir, template_name, version, job_description, job_name, account, nb_cpus, wallclock_limit, exe_time, mail, memory, additional_lines):
    j2_env = Environment(loader=FileSystemLoader(template_dir), trim_blocks=True)
    header = j2_env.get_template(template_name).render(version=version, job_description=job_description,
                                                     job_name=job_name, account=account, nb_cpus=nb_cpus,
                                                     wallclock_limit=wallclock_limit, exe_time=exe_time, mail=mail,
                                                     memory=memory,
                                                     additional_lines=additional_lines)
    return header


def generate_context(template_dir, template_name, sim_dirs, obs_dirs, station_file, rrup_files, output_dir, np, extended, simple, comp,mgmt_db):
    j2_env = Environment(loader=FileSystemLoader(template_dir), trim_blocks=True)
    context = j2_env.get_template(template_name).render(
        time=time, comp=comp,
        sim_dirs=sim_dirs, obs_dirs=obs_dirs,
        rrup_files=rrup_files, station_file=station_file,
        output_dir=output_dir, np=np, extended=extended, simple=simple,mgmt_db_location=mgmt_db)
    return context


def generate_sl(sim_dirs, obs_dirs, station_file, rrup_files, output_dir, prefix, i, np, extended, simple, comp, version,
                job_description, job_name, account, nb_cpus, wallclock_limit, exe_time, mail, memory, additional_lines,mgmt_db):

    template_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', TEMPLATES_DIR)
    header = generate_header(template_path, HEADER_TEMPLATE, version, job_description, job_name, account, nb_cpus, wallclock_limit, exe_time, mail, memory, additional_lines)
    context = generate_context(template_path, CONTEXT_TEMPLATE, sim_dirs, obs_dirs, station_file, rrup_files, output_dir, np, extended, simple, comp, mgmt_db)
    sl_name = SL_NAME.format(prefix, i)
    return sl_name, "{}\n{}".format(header, context)

def update_db(process, status, mgmt_db_location, srf_name, jobid):
    db_queue_path = os.path.join(mgmt_db_location,"mgmt_db_queue")
    cmd_name = os.path.join(db_queue_path, "%s_%s_q"%(timestamp,jobid))
    #TODO: change this to use python3's format()
    cmd = "python $gmsim/workflow/scripts/management/update_mgmt_db.py %s %s %s --run_name %s  --job %s"%(mgmt_db_location, process, status, srf_name, jobid)
    with open(cmd_name, 'w+') as f:
        f.write(cmd)
        f.close()
#    db = db_helper.connect_db(mgmt_db_location)
#    while True:
#        try:
#            update_mgmt_db.update_db(db, process, status, job=jobid, run_name=srf_name)
#        except:
#            print("en error occured while trying to update DB, re-trying")
#            sleep(10)
#        else:
#            break
#    db.connection.commit()
#    db.connection.close()

def write_sl(name_context_list, submit=False, mgmt_db_location=None):
    for sl_name, context,srf_list in name_context_list:
        with open(sl_name, 'w') as sl:
            print("writing {}".format(sl_name))
            sl.write(context)
        if submit is True:
            fname_sl_real_path = os.path.realpath(sl_name)
            jobid = submit_sl_script(fname_sl_real_path)
            if jobid != None and mgmt_db_location != None:
                for srf_name in srf_list:
                    update_db("IM_calculation", "queued", mgmt_db_location, srf_name, jobid)

def submit_sl_script(script):
    if type(script) == unicode:
        script = script.encode()
    print "Submitting %s" % script
    res = exe("sbatch %s" % script, debug=False)
    if len(res[1]) == 0:
        # no errors, return the job id
        return res[0].split()[-1] 
    else:
        print res
        return None


def get_basename_without_ext(path):
    return os.path.splitext(os.path.basename(path))[0]


def get_fault_name(run_name):
    return run_name.split('_')[0]


def split_and_generate_slurms(sim_dirs, obs_dirs, station_file, rrup_files, output_dir, processes, max_lines, prefix,
                              extended='', simple='', comp=COMPS[0], version=VERSION, job_description='', job_name=JOB,
                              account=ACCOUNT, nb_cpus=NTASKS, wallclock_limit=TIME, exe_time=EXE_TIME, mail=MAIL,
                              memory=MEMORY, additional_lines=ADDI,mgmt_db=None):
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
        name, context = generate_sl(sim_dirs[i: last_line_index], obs_dirs[i: last_line_index], station_file,
                                    rrup_files[i: last_line_index], output_dir, prefix, i, processes, extended, simple, comp, version,
                job_description, job_name, account, nb_cpus, wallclock_limit, exe_time, mail, memory, additional_lines,mgmt_db)
        #append the name of sl, content of sl and related srf names
        if not sim_dirs:
            srf_name = None
        else:
            srf_name = zip(*sim_dirs)[1][i: last_line_index]
        name_context_list.append((name, context, srf_name))
        i += max_lines

    return name_context_list


def get_fd_path(srf_filepath, sim_dir):
    fd = ''
    run_name = get_fault_name(get_basename_without_ext(srf_filepath))
    if sim_dir is not None:
        fault_dir = os.path.join(sim_dir, run_name)
        try:
            fd_path = utils.load_yaml(os.path.join(fault_dir, 'sim_params.yaml'))['FD_STATLIST']
            fd = "-fd {}".format(fd_path)
        except Exception as e:
            fd = SKIP
    return fd


def get_dirs(run_folder, arg_identifiers, com_pattern):
    dirs = []
    for identifier in arg_identifiers:
        fault_name = get_fault_name(identifier)
        #dir_path = glob.glob(os.path.join(run_folder, com_pattern.format(fault_name, identifier)))
        dir_path = os.path.join(run_folder, com_pattern.format(fault_name, identifier))
        if glob.glob(dir_path) == []:
            print("{} does not exists".format(dir_path))
        else:
            dirs.append(dir_path)
    return dirs


def main():
    parser = argparse.ArgumentParser(description="Prints out a slurm script to run IM Calculation over a run-group")
    parser.add_argument('-sim', '--sim_dir',
                        help="Path to sim-run-group containing faults and acceleration in the subfolder */BB/*/*")
    parser.add_argument('-obs', '--obs_dir',
                        help="Path to obs-run-group containing faults and accelerations in the subfolder */*/accBB")
    parser.add_argument('-srf', '--srf_dir',
                        help="Path to run-group containing the srf files in the path matching */Srf/*.srf")
    parser.add_argument('-ll', '--station_file',
                        help="Path to a single station file for ruputure distance calculations")
    parser.add_argument('-np', '--processes', default=DEFAULT_N_PROCESSES, help="number of processors to use")
    parser.add_argument('-ml', '--max_lines', default=100, type=int,
                        help="maximum number of lines in a slurm script. Default 100")
    parser.add_argument('-e', '--extended_period', action='store_const', const='-e', default='',
                        help="add '-e' to indicate the use of extended pSA period. Default not using")
    parser.add_argument('-s', '--simple_output', action='store_const', const='-s', default='',
                        help="Please add '-s' to indicate if you want to output the big summary csv only(no single station csvs). Default outputting both single station and the big summary csvs")
    parser.add_argument('-o', '--rrup_out_dir', default=DEFAULT_RRUP_OUTDIR,
                        help="output directory to store rupture distances output.Default is {}".format(
                            DEFAULT_RRUP_OUTDIR))
    parser.add_argument('-c', '--comp', default=COMPS[0], help="specify which verlocity compoent to calculate. choose from {}. Default is {}".format(COMPS, COMPS[0]))
    parser.add_argument('-i', '--identifiers', default='*', nargs='+',
                        help="a list of space-seperated unique runnames of the simulations.eg.'Albury_HYP01-01_S1244 OpouaweUruti_HYP44-47_S1674'")
    parser.add_argument('-t', '--time', default=TIME,
                        help="estimated running time for each slurm sciprt. must be in 'hh:mm:ss' format. Default is {}".format(TIME))
    parser.add_argument('--version', default=VERSION, help="default version is 'slurm'")
    parser.add_argument('--job_description', default=JOB, help="job description of slurm script. Default is {}".format(JOB))
    parser.add_argument('--job_name', default=JOB, help="job name of slurm script. Default is {}".format(JOB))
    parser.add_argument('--account', default=ACCOUNT, help="Account name. Default is {}.".format(ACCOUNT))
    parser.add_argument('--ntasks', default=NTASKS, type=int, help="number of tasks per node. Default is {}".format(NTASKS))
    parser.add_argument('--exe_time', default=EXE_TIME, help="Default is {}".format(EXE_TIME.replace('%','%%')))
    parser.add_argument('--mail', default=MAIL, help="Default is {}".format(MAIL))
    parser.add_argument('--memory', default=MEMORY, help="Memory per cpu. Default is {}".format(MEMORY))
    parser.add_argument('--additional_lines', default=ADDI, help="additional lines add to slurm header. Default is {}".format(ADDI))
    parser.add_argument('--mgmt_db', type=str, help='path to the mgmt_db')

    parser.add_argument('--auto',action='store_true',help='submit the sl script as well')

    args = parser.parse_args()

    if args.srf_dir is not None:
        utils.setup_dir(args.rrup_out_dir)

    if args.max_lines <= 0:
        parser.error("-ml argument should come with a number that is 0 < -ml <= (max_lines-header_and_other_prints) allowed by slurm")

    if not re.match(TIME_REGEX, args.time):
        parser.error("time must be in 'hh:mm:ss' format and not exceeding 24:00:00")

    if not args.comp in COMPS:
        parser.error("verlocity component must be in {} where ellipsis means calculating all compoents".format(COMPS))

    # sim_dir = /nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p5/Runs
    if args.sim_dir is not None:
        sim_waveform_dirs = get_dirs(args.sim_dir, args.identifiers, '{}/{}/BB')
       # sim_waveform_dirs = checkpoint.checkpoint_sim_obs(sim_waveform_dirs, '../../../IM_calc/')  # return dirs that are not calculated yet
        sim_waveform_dirs = checkpoint.checkpoint_wrapper(args.sim_dir, sim_waveform_dirs,'s')
        sim_run_names = map(os.path.basename, map(os.path.dirname, sim_waveform_dirs))
        sim_faults = map(get_fault_name, sim_run_names)
        sim_dirs = zip(sim_waveform_dirs, sim_run_names, sim_faults)
        # sim
        name_context_list = split_and_generate_slurms(sim_dirs, [], args.station_file, [], args.rrup_out_dir,
                                                      args.processes,
                                                      args.max_lines, 'sim', extended=args.extended_period,
                                                      simple=args.simple_output, comp=args.comp, version=args.version, job_description=args.job_description, job_name=args.job_name,
                              account=args.account, nb_cpus=args.ntasks, wallclock_limit=args.time, exe_time=args.exe_time, mail=args.mail,
                              memory=args.memory, additional_lines=args.additional_lines,mgmt_db=args.mgmt_db)
        write_sl(name_context_list,args.auto, args.mgmt_db)

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
        name_context_list = split_and_generate_slurms([], [], args.station_file, rrup_files,
                                                      args.rrup_out_dir, args.processes, args.max_lines, 'rrup', version=args.version, job_description=args.job_description, job_name=args.job_name,
                              account=args.account, nb_cpus=args.ntasks, wallclock_limit=args.time, exe_time=args.exe_time, mail=args.mail,
                              memory=args.memory, additional_lines=args.additional_lines)
        write_sl(name_context_list)

    if args.obs_dir is not None:
        obs_waveform_dirs = glob.glob(os.path.join(args.obs_dir, '*'))
       # obs_waveform_dirs = checkpoint.checkpoint_sim_obs(obs_waveform_dirs, '../IM_calc/')
        obs_waveform_dirs = checkpoint.checkpoint_wrapper(args.obs_dir, obs_waveform_dirs,'o')
        obs_run_names = map(os.path.basename, obs_waveform_dirs)
        obs_faults = map(get_fault_name, obs_run_names)
        obs_dirs = zip(obs_waveform_dirs, obs_run_names, obs_faults)
        # obs
        name_context_list = split_and_generate_slurms([], obs_dirs, args.station_file, [], args.rrup_out_dir,
                                                      args.processes,
                                                      args.max_lines, 'obs', extended=args.extended_period,
                                                      simple=args.simple_output, comp=args.comp, version=args.version, job_description=args.job_description, job_name=args.job_name,
                              account=args.account, nb_cpus=args.ntasks, wallclock_limit=args.time, exe_time=args.exe_time, mail=args.mail,
                              memory=args.memory, additional_lines=args.additional_lines)
        write_sl(name_context_list)


if __name__ == '__main__':
    main()


