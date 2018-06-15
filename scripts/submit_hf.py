import glob
import os.path
from os.path import basename
import sys

sys.path.append(os.path.abspath(os.path.curdir))
import params
import params_base
import params_base_bb

import fnmatch
from math import ceil
import argparse

import qcore.srf
import qcore.shared
#default values
default_version='run_hf_mpi'
default_core="80"
default_run_time="00:30:00"
default_memory="16G"
default_account='nesi00213'
#TODO:this number needs to find a way to update more frequently, based on stored WCT.
default_hf_coef=1741000000
default_scale=1.2
#datetime related
from datetime import datetime 
timestamp_format = "%Y%m%d_%H%M%S"
timestamp = datetime.now().strftime(timestamp_format)

from management import create_mgmt_db
from management import update_mgmt_db

# TODO: move this to qcore library
from temp_shared import resolve_header
from shared_workflow.shared import *


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


# TODO, probably move this to qcore lib
def est_core_hours_hf(timestep, station_count, sub_fault_count, hf_coef):
    total_size = timestep * station_count * sub_fault_count
    core_hours = round(total_size / hf_coef, 2)
    return core_hours


def est_wct(est_core_hours, ncore, scale):
    scaled_est = est_core_hours * scale
    if scaled_est <= 0:
        scaled_est = 1
    time_per_cpu = ceil(float(scaled_est) / float(ncore))
    estimated_wct = '{0:02.0f}:{1:02.0f}:00'.format(*divmod(time_per_cpu * 60, 60))
    return estimated_wct


def update_db(process, status, mgmt_db_location, srf_name, jobid):
    db = create_mgmt_db.connect_db(mgmt_db_location)
    update_mgmt_db.update_db(db, process, status, job=jobid, run_name=srf_name)
    db.connection.commit()


def write_sl_script(hf_sim_dir, sim_dir, hf_run_name, stoch_name, sl_template_prefix, hf_option, nb_cpus=default_core,
                    run_time=default_run_time, memory=default_memory, account=default_account, binary=False):
    from params_base import mgmt_db_location

    f_template = open('%s.sl.template' % sl_template_prefix)
    template = f_template.readlines()
    str_template = ''.join(template)

    if binary:
        create_dir = "mkdir -p " + os.path.join(hf_sim_dir, "Acc") + "\n"
        hf_submit_command = create_dir + "srun python $BINPROCESS/hf_sim.py "
        arguments_for_hf = [params_base.hf_slips[0], params_base.FD_STATLIST, os.path.join(hf_sim_dir, "Acc/HF.bin"),
                            "-m", params_base_bb.hf_v_model, "--duration", params_base.sim_duration, "--dt", params.hf_dt]

        hf_submit_command += " ".join(map(str, arguments_for_hf))
        if hf_option == 1:
            hf_submit_command += "-i"
    else:
        hf_submit_command = "srun python  $BINPROCESS/hfsims-stats-mpi.py " + hf_sim_dir + " " + str(hf_option)

    txt = str_template.replace("{{hf_sim_dir}}", hf_sim_dir)
    txt = txt.replace("{{mgmt_db_location}}", mgmt_db_location)
    txt = txt.replace("{{hf_submit_command}}", hf_submit_command)
    txt = txt.replace("{{sim_dir}}", sim_dir).replace("{{hf_run_name}}", hf_run_name).replace("{{srf_name}}",
                                                                                              stoch_name)
    #replacing the name of test scipts
    if binary:
        txt = txt.replace("{{test_hf_script}}","test_hf_binary.sh")
    else:
        txt = txt.replace("{{test_hf_script}}","test_hf_ascii.sh")

    variation = stoch_name.replace('/', '__')
    print variation

    fname_sl_script = '%s_%s_%s.sl' % (sl_template_prefix, variation, timestamp)
    f_sl_script = open(fname_sl_script, 'w')
    job_name = "sim_hf.%s" % variation

    header = resolve_header(account, nb_cpus, run_time, job_name, "slurm", memory, timestamp,
                            job_description="HF calculation", additional_lines="###SBATCH -C avx")
    f_sl_script.write(header)
    f_sl_script.write(txt)
    f_sl_script.close()
    fname_sl_abs_path = os.path.join(os.path.abspath(os.path.curdir), fname_sl_script)
    print "Slurm script %s written" % fname_sl_abs_path
    generated_script = fname_sl_abs_path

    return generated_script


if __name__ == '__main__':
    # parse the arguments
    parser = argparse.ArgumentParser()
    # if some reason user decide to use different version, instead of mpi
    parser.add_argument('--version', type=str, default=None, const=None)
    # optional args for ncore, WCT, etc.
    parser.add_argument('--ncore', type=str, default=default_core)
    parser.add_argument('--wct', type=str, nargs='?', default=None, const=None)
    # the const of auto is set to True, so that as long as --auto is used, no more value needs to be provided
    parser.add_argument('--auto', type=int, nargs='?', default=None, const=True)
    parser.add_argument('--est_wct', type=int, nargs='?', default=None, const=True)
    # rand_reset, if somehow the user decide to use it but not defined in params_base_bb
    # the const is set to True, so that as long as --rand_reset is used, no more value needs to be provided
    parser.add_argument('--rand_reset', type=int, nargs='?', default=None, const=True)
    parser.add_argument('--site_specific', type=int, nargs='?', default=None, const=True)
    parser.add_argument('--account', type=str, default=default_account)
    parser.add_argument('--srf', type=str, default=None)
    parser.add_argument('--binary', action="store_true")
    args = parser.parse_args()
    # check if parsed ncore
    if args.ncore != default_core:
        ncore = args.ncore
    else:
        ncore = default_core

    # check if the args is none, if not, change the version
    if args.version != None:
        version = args.version
        if version == 'serial' or version == 'run_hf':
            ll_name_prefix = 'run_hf'
            ncore = "1"
        if version == 'mp' or version == 'run_hf_mp':
            wl_name_prefix = 'run_hf_mp'
        elif version == 'mpi' or version == 'run_hf_mpi':
            ll_name_prefix = 'run_hf_mpi'
        else:
            print '% cannot be recognize as a valide option' % version
            print 'version is set to default: %', default_version
            version = default_version
            ll_name_prefix = default_version
    else:
        version = default_version
        ll_name_prefix = default_version
    print "version:", version
    # if auto flag is set to true, auto estimate the WCT and use default cores(or get from --ncore)

    # check rand_reset
    if args.site_specific != None or params_base_bb.site_specific:
        print "Note: site_specific = True, rand_reset = True"
        hf_option = 2
    else:
        try:
            if args.rand_reset != None or params_base_bb.rand_reset:
                hf_option = 1
        except:
            hf_option = 0
            print "Note: rand_reset is not defined in params_base_bb.py. We assume rand_reset=%s" % bool(hf_option)

    # est_wct and submit, if --auto used
    if args.auto != None:
        args.est_wct = True
        submit_yes = True
    else:
        # None: ask user if want to submit; False: dont submit
        submit_yes = confirm("Also submit the job for you?")

    print "account:", args.account

    # modify the logic to use the same as in install_bb:
    # sniff through params_base to get the names of srf, instead of running throught file directories.

    # loop through all srf file to generate related slurm scripts
    counter_srf = 0
    for srf in params.srf_files:
        srf_name = os.path.splitext(basename(srf))[0]
        # if srf(variation) is provided as args, only create the slurm with same name provided
        if args.srf != None and srf_name != args.srf:
            continue
            # --est_wct used, automatically assign run_time using estimation
        if args.est_wct != None:
            timesteps= float(params.sim_duration)/float(params.hf_dt)
            #get station count
            station_count = len(qcore.shared.get_stations(params.FD_STATLIST))
            print station_count
            #get the number of sub faults for estimation
            #TODO:make it read through the whole list instead of assuming every stoch has same size
            sub_fault_count,sub_fault_area=qcore.srf.get_nsub_stoch(params.hf_slips[counter_srf],get_area=True)
            print "sb:",sub_fault_area
            est_chours = est_core_hours_hf(timesteps,station_count,sub_fault_area,default_hf_coef)
            print est_chours
            print "The estimated time is currently not so accurate."
            run_time = est_wct(est_chours, ncore, default_scale)
        else:
            run_time = default_run_time
        hf_sim_dir = os.path.join(os.path.join(params.hf_dir, params_base_bb.hf_run_names[counter_srf]), srf_name)
        sim_dir = params.sim_dir
        hf_run_name = params_base_bb.hf_run_names[counter_srf]
        created_script = write_sl_script(hf_sim_dir, sim_dir, hf_run_name, srf_name, ll_name_prefix, hf_option, ncore,
                                         run_time, account=args.account, binary=args.binary)
        jobid = submit_sl_script(created_script, submit_yes)
        if jobid != None:
            update_db("HF", "in-queue", params.mgmt_db_location, srf_name, jobid)

        counter_srf += 1
