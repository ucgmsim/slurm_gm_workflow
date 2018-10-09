
bin_process_path = '/nesi/transit/nesi00213/workflow'
import glob
import os.path
import sys
import fnmatch
import os
from os.path import basename

sys.path.append(os.path.abspath(os.path.curdir))
import params
import params_base_bb

# datetime related
from datetime import datetime

timestamp_format = "%Y%m%d_%H%M%S"
timestamp = datetime.now().strftime(timestamp_format)

default_account = 'nesi00213'
default_version = 'run_bb_mpi'
default_core = "80"
default_run_time = "00:30:00"
default_memory = "16G"

import argparse

# TODO: move this to qcore library
from temp_shared import resolve_header
from shared_workflow.shared import *


from management import update_mgmt_db
from management import db_helper


def confirm(q):
    show_horizontal_line()
    print q
    return show_yes_no_question()


def submit_sl_script(script, submit_yes=None):
    # print "Submitting is not implemented yet!"
    # if submit_yes == None:
    #    submit_yes = confirm("Also submit the job for you?")
    if submit_yes:
        # encode if it is unicode
        # TODO:fix this in qcore.shared.exe()
        if type(script) == unicode:
            script = script.encode()
        print "Submitting %s" % script
        res = exe("sbatch %s" % script, debug=False)
        if len(res[1]) == 0:
            # no errors, return the job id
            return res[0].split()[-1]
    else:
        print "User chose to submit the job manually"
        return None


def update_db(process, status, mgmt_db_location, srf_name, jobid):
    db = db_helper.connect_db(mgmt_db_location)
    update_mgmt_db.update_db(db, process, status, job=jobid, run_name=srf_name)
    db.connection.commit()


def write_sl_script(bb_sim_dirs, sim_dir, hf_run_name, srf_name, sl_template_prefix, nb_cpus=default_core,
                    run_time=default_run_time, memory=default_memory, account=default_account, binary=False):
    from params_base import mgmt_db_location

    f_template = open('%s.sl.template' % sl_template_prefix)
    template = f_template.readlines()
    str_template = ''.join(template)
    if binary:
        create_directory = "mkdir -p " + os.path.join(params.bb_dir, hf_run_name, srf_name, "Acc") + "\n"
        submit_command = create_directory + "srun python $BINPROCESS/bb_sim.py "
        arguments = [os.path.join(params.lf_sim_root_dir, srf_name + "/OutBin"), params.vel_mod_dir,
                     os.path.join(params.hf_dir, hf_run_name, srf_name, "Acc/HF.bin"),
                     params.stat_vs_est, os.path.join(params.bb_dir, hf_run_name, srf_name, "Acc/BB.bin"),
                     "--flo", params.flo]
        txt = str_template.replace("{{bb_submit_command}}", submit_command + " ".join(arguments))
    else:
        txt = str_template.replace("{{bb_submit_command}}",
                                   "srun python  $BINPROCESS/match_seismo-mpi.py " + bb_sim_dir)

    #    variation = '_'.join(bb_sim_dir.split('/')[0:2])
    variation = srf_name.replace('/', '__')
    print variation

    txt = txt.replace("$rup_mod", variation)
    txt = txt.replace("{{mgmt_db_location}}", mgmt_db_location)
    txt = txt.replace("{{sim_dir}}", sim_dir).replace("{{hf_run_name}}", hf_run_name).replace("{{srf_name}}", srf_name)

    #replace the name of test script
    if binary:
        txt = txt.replace("{{test_bb_script}}","test_bb_binary.sh")
    else:
        txt = txt.replace("{{test_bb_script}}","test_bb_ascii.sh")

    fname_sl_script = '%s_%s_%s.sl' % (sl_template_prefix, variation, timestamp)
    f_sl_script = open(fname_sl_script, 'w')

    # TODO: change this values to values that make more sense or come from somewhere
#    nb_cpus = "80"
#    run_time = "00:30:00"
    job_name = "sim_bb_%s" % variation
#    memory = "16G"
    header = resolve_header(account, nb_cpus, run_time, job_name, "slurm", memory, timestamp,
                            job_description="BB calculation", additional_lines="##SBATCH -C avx")
    f_sl_script.write(header)
    f_sl_script.write(txt)
    f_sl_script.close()

    fname_sl_abs_path = os.path.join(os.path.abspath(os.path.curdir), fname_sl_script)
    print "Slurm script %s written" % fname_sl_abs_path
    generated_script = fname_sl_abs_path
    return generated_script


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--auto", nargs="?", type=str, const=True)
    parser.add_argument('--version', type=str, default=default_version)
    parser.add_argument('--account', type=str, default=default_account)
    parser.add_argument('--est_wct', type=int, nargs='?', default=None, const=True)
    parser.add_argument('--srf', type=str, default=None)
    parser.add_argument('--binary', action="store_true")
    args = parser.parse_args()

    # version = 'MPI'
    # sl_name_prefix = 'run_bb_mpi'
    # if len(sys.argv) == 2:
    #    version = sys.argv[1]
    #    if version == 'MPI':
    #        sl_name_prefix = 'run_bb_mpi'
    #    else:
    #        print 'Set to default %s' % version
    if args.version != None:
        version = args.version
        if version == 'serial' or version == 'run_bb':
            sl_name_prefix = 'run_bb'
            ncore = "1"
        elif version == 'mp' or version == 'run_bb_mp':
            sl_name_prefix = 'run_bb_mp'
        elif version == 'mpi' or version == 'run_bb_mpi':
            sl_name_prefix = 'run_bb_mpi'
        else:
            print '% cannot be recognize as a valide option' % version
            print 'version is set to default: %', default_version
            version = default_version
            sl_name_prefix = default_version
    else:
        version = default_version
        sl_name_prefix = default_version

    print version

    # est_wct and submit, if --auto used
    if args.auto != None:
        args.est_wct = True
        submit_yes = True
    else:
        # None: ask user if want to submit; False: dont submit
        submit_yes = confirm("Also submit the job for you?")

    counter_srf = 0
    for srf in params.srf_files:
        srf_name = os.path.splitext(basename(srf))[0]
        # if srf(variation) is provided as args, only create the slurm with same name provided
        if args.srf != None and srf_name != args.srf:
            continue
        # --est_wct used, automatically assign run_time using estimation
        if args.est_wct != None:
            # TODO:enable time estimation after WCT is properly implemented
            run_time = default_run_time
            # timesteps= float(params.sim_duration)/float(params.hf_dt)
            # get station count
            # station_count = len(get_stations(params.FD_STATLIST))
            # print station_count
            # get the number of sub faults for estimation
            # TODO:make it read through the whole list instead of assuming every stoch has same size
            # sub_fault_count,sub_fault_area=get_nsub_stoch(params.hf_slips[counter_srf],get_area=True)
            # print "sb:",sub_fault_area
            # est_chours = est_core_hours_hf(timesteps,station_count,sub_fault_area,default_hf_coef)
            # print est_chours
            # print "The estimated time is currently not so accurate."
            # run_time = est_wct(est_chours,ncore,default_scale)
        else:
            run_time = default_run_time
        bb_sim_dir = os.path.join(os.path.join(params.bb_dir, params_base_bb.hf_run_names[counter_srf]), srf_name)
        hf_run_name = params_base_bb.hf_run_names[counter_srf]
        sim_dir = params.sim_dir
        created_script = write_sl_script(bb_sim_dir, sim_dir, hf_run_name, srf_name, sl_name_prefix,
                                         account=args.account, binary=args.binary)
        jobid = submit_sl_script(created_script, submit_yes)
        if jobid != None:
            update_db("BB", "queued", params.mgmt_db_location, srf_name, jobid)
        elif submit_yes == True and jobid == None:
            print "there is error while trying to submit the slurm script, please manual check for errors"

        counter_srf += 1
