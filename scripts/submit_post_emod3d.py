import glob
import os.path
from os.path import basename
import sys
import math

# TODO: remove this once temp_shared is gone
from temp_shared import resolve_header

from shared_workflow.shared import *

sys.path.append(os.getcwd())
# from params_base import tools_dir
# from params_base import sim_dir
# from params_base import mgmt_db_location

from management import db_helper
from management import update_mgmt_db

#datetime related
from datetime import datetime
timestamp_format = "%Y%m%d_%H%M%S"
timestamp = datetime.now().strftime(timestamp_format)


merge_ts_name_prefix = "post_emod3d_merge_ts"
winbin_aio_name_prefix = "post_emod3d_winbin_aio"

#TODO: implement estimation for these numbers
default_run_time_merge_ts="00:30:00"
default_run_time_winbin_aio="02:00:00"
# default_core_merge_ts must be 4, higher number of cpu cause un-expected errors (TODO: maybe fix it when merg_ts's time become issue)
default_core_merge_ts = "4"
default_core_winbin_aio = "80"
default_memory="16G"
default_account='nesi00213'
#TODO:the max number of cpu per node may need update when migrate machines
#this variable is critical to prevent crashes for winbin-aio
max_tasks_per_node = "80"

import argparse

from qcore import utils


def get_seis_len(seis_path):
    filepattern = os.path.join(seis_path, '*_seis*.e3d')
    seis_file_list = sorted(glob.glob(filepattern))
    return len(seis_file_list)


def confirm(q):
    show_horizontal_line
    print q
    return show_yes_no_question()


def submit_sl_script(script, submit_yes=None):
    #print "Submitting is not implemented yet!"
    print script
    #if submit_yes == None:
    #    submit_yes = confirm("Also submit the job for you?")
    if submit_yes:
        #encode if it is unicode
        #TODO:fix this in qcore.shared.exe()
        if type(script) == unicode:
            script = script.encode()
        print "Submitting %s" %script
        res = exe("sbatch %s"%script, debug=False)
        if len(res[1]) == 0:
            #no errors, return the job id
            return res[0].split()[-1]
    else:
        print "User chose to submit the job manually"
        return None


def write_sl_script_merge_ts(lf_sim_dir, sim_dir, tools_dir, mgmt_db_location, rup_mod, run_time=default_run_time_merge_ts, nb_cpus=default_core_merge_ts , memory=default_memory,account=default_account):
    # reading merge_ts_template
    merge_ts_template = open('%s.sl.template' % merge_ts_name_prefix)
    merge_ts_template_contents = merge_ts_template.readlines()
    merge_ts_str_template = ''.join(merge_ts_template_contents)
    
    # TODO: the merge_ts binrary needed to use relative path instead of absolute, maybe fix this
    txt = merge_ts_str_template.replace("{{lf_sim_dir}}", os.path.relpath(lf_sim_dir,sim_dir))
    try:
        txt = txt.replace("{{tools_dir}}", tools_dir)
    except:
        print "**error while replacing tools_dir**"
    txt = txt.replace("{{mgmt_db_location}}", mgmt_db_location)    
    txt = txt.replace("{{sim_dir}}",sim_dir).replace("{{srf_name}}",rup_mod)

    job_name = "post_emod3d.merge_ts.%s" % rup_mod
    header = resolve_header(args.account, nb_cpus, run_time, job_name, "Slurm", memory,timestamp,
                            job_description="post emod3d: merge_ts", additional_lines="###SBATCH -C avx")

    fname_merge_ts_script = '%s_%s_%s.sl' % (merge_ts_name_prefix, rup_mod,timestamp)
    final_merge_ts = open(fname_merge_ts_script, 'w')
    final_merge_ts.write(header)
    final_merge_ts.write(txt)
    final_merge_ts.close()
    print "Slurm script %s written" % fname_merge_ts_script
   
    fname_sl_abs_path = os.path.join(os.path.abspath(os.path.curdir),fname_merge_ts_script) 
    generated_script =  fname_sl_abs_path
    return generated_script


def write_sl_script_winbin_aio(lf_sim_dir, sim_dir, mgmt_db_location, rup_mod, run_time = default_run_time_winbin_aio, nb_cpus=default_core_winbin_aio,memory=default_memory,account=default_account):

    # reading winbin_aio_template
    winbin_aio_template = open('%s.sl.template' % winbin_aio_name_prefix)
    winbin_aio_template_contents = winbin_aio_template.readlines()
    winbin_aio_str_template = ''.join(winbin_aio_template_contents)

    # preparing winbin_aio
    # TODO: the merge_ts binrary needed to use relative path instead of absolute, maybe fix this
    txt = winbin_aio_str_template.replace("{{lf_sim_dir}}", os.path.relpath(lf_sim_dir, sim_dir))
    txt = txt.replace("{{mgmt_db_location}}", mgmt_db_location)
    txt = txt.replace("{{sim_dir}}",sim_dir).replace("{{srf_name}}", rup_mod)
    #get the file count of seis files
    path_outbin=os.path.join(os.path.join(sim_dir,lf_sim_dir),"OutBin")
    sfl_len=int(get_seis_len(path_outbin))
    #round down to the max cpu per node
    nodes = int(round( (sfl_len/int(max_tasks_per_node)) - 0.5))
    if nodes <= 0:
        #use the same cpu count as the seis files
        nb_cpus=str(sfl_len)
    else:
        nb_cpus = str(nodes*int(max_tasks_per_node))

    job_name = "post_emod3d.winbin_aio.%s" % rup_mod
    header = resolve_header(args.account, nb_cpus, run_time, job_name, "slurm", memory,timestamp,
                            job_description="post emod3d: winbin_aio", additional_lines="###SBATCH -C avx")

    fname_winbin_aio_script = '%s_%s_%s.sl' % (winbin_aio_name_prefix, rup_mod, timestamp)
    final_winbin_aio = open(fname_winbin_aio_script, 'w')
    final_winbin_aio.write(header)
    final_winbin_aio.write(txt)
    final_winbin_aio.close()
    print "Slurm script %s written" % fname_winbin_aio_script
    fname_sl_abs_path = os.path.join(os.path.abspath(os.path.curdir),fname_winbin_aio_script)
    generated_script = fname_sl_abs_path
    return generated_script


def update_db(process, status, mgmt_db_location, srf_name,jobid):
    db = db_helper.connect_db(mgmt_db_location)
    update_mgmt_db.update_db(db, process, status, job=jobid, run_name=srf_name)
    db.connection.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--auto", nargs="?", type=str,const=True)
    parser.add_argument('--account', type=str, default=default_account)
    parser.add_argument('--merge_ts', nargs="?", type=str,const=True)
    parser.add_argument('--winbin_aio', nargs="?", type=str,const=True)
    parser.add_argument('--srf',type=str,default=None)
    parser.add_argument('--pre_hf', nargs="?", type=str,const=True)

    args = parser.parse_args()

    created_scripts = []
    try:
        params = utils.load_params('fault_params.yaml')
    except:
        print "load params failed."
        sys.exit()
    else:
        wct_set=False 
        if args.auto == True:
            submit_yes = True
        else:
            submit_yes = confirm("Also submit the job for you?")
        for srf in params.srf_file:
            #get the srf(rup) name without extensions
            srf_name = os.path.splitext(basename(srf))[0]
            #if srf(variation) is provided as args, only create the slurm with same name provided
            if args.srf != None and srf_name != args.srf:
                continue
            #get lf_sim_dir

            lf_sim_dir = params.lf_sim_root_dir
            sim_dir = params.sim_dir

            #TODO: update the script below when implemented estimation WCT
            #nx = int(params.nx)
            #ny = int(params.ny)
            #nz = int(params.nz)
            #dt = float(params.dt)
            #sim_duration = float(params.sim_duration)
            #default_core will be changed is user pars ncore
            #num_procs = default_core
            #total_est_core_hours= est_e3d.est_core_hours_emod3d(nx,ny,nz,dt,sim_duration)
            #estimated_wct = est_e3d.est_wct(total_est_core_hours,num_procs, default_wct_scale)
            #print "Estimated WCT (scaled and rounded up):%s"%estimated_wct
            
            #run merge_ts related scripts only
            #if non of args are provided, run script related to both
            if args.winbin_aio != True and args.merge_ts != True:
                args.merge_ts = True
                args.winbin_aio = True

            if args.merge_ts == True:
                created_script = write_sl_script_merge_ts(lf_sim_dir, params.sim_dir, params.tools_dir, params.mgmt_db_location, srf_name)
                jobid = submit_sl_script(created_script,submit_yes)
                if jobid != None:
                    update_db("merge_ts","queued",params.mgmt_db_location, srf_name, jobid)
            #run winbin_aio related scripts only
            if args.winbin_aio == True:
                created_script = write_sl_script_winbin_aio(lf_sim_dir, params.sim_dir, params.mgmt_db_location, srf_name)
                jobid = submit_sl_script(created_script,submit_yes)
                if jobid != None:
                    update_db("winbin_aio", "queued", params.mgmt_db_location, srf_name, jobid)
