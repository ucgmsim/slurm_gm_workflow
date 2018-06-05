# TODO: import the CONFIG here

import glob
import os.path
from os.path import basename
import sys
import os
import set_runparams

from management import create_mgmt_db
from management import update_mgmt_db

#sys.path.append(os.path.abspath(os.path.curdir))

from qcore.shared import *
import estimate_emod3d as est_e3d

#datetime related
from datetime import datetime 
timestamp_format = "%Y%m%d_%H%M%S"
timestamp = datetime.now().strftime(timestamp_format)

#default values
default_core="160"
default_run_time="02:00:00"
default_memory="16G"
default_account='nesi00213'
#default_emod3d_coef=3.00097
#coef should be predefined in est_emod3d.py
default_ch_scale=1.1
default_wct_scale=1.2

# TODO: remove this once temp_shared is gone
from temp_shared import resolve_header


import install

# section for parser to determine if using automate wct
import argparse




def confirm(q):
    show_horizontal_line()
    print q
    return show_yes_no_question()

def submit_sl_script(script,submit_yes=None):
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

    
    
def write_sl_script(lf_sim_dir,sim_dir,srf_name, run_time=default_run_time, nb_cpus=default_core,memory=default_memory,account=default_account):

    from params_base import tools_dir
    from params_base import mgmt_db_location

    set_runparams.create_run_parameters(srf_name)
    generated_scripts = []

    f_template = open('run_emod3d.sl.template')
    template = f_template.readlines()
    str_template = ''.join(template)
    
    txt = str_template.replace("{{lf_sim_dir}}", lf_sim_dir).replace("{{tools_dir}}", tools_dir)
    txt = txt.replace("{{mgmt_db_location}}", mgmt_db_location)
    txt = txt.replace("{{sim_dir}}", sim_dir).replace("{{srf_name}}",srf_name)
    fname_slurm_script = 'run_emod3d_%s_%s.sl' % (srf_name,timestamp)
    f_sl_script = open(fname_slurm_script, 'w')

    # slurm header
    # TODO: this value has to change accordingly to the value used for WCT estimation
    job_name = "run_emod3d.%s" % srf_name
    header = resolve_header(args.account, nb_cpus, run_time, job_name, "slurm", memory, timestamp,
                            job_description="emod3d slurm script", additional_lines="#SBATCH --hint=nomultithread")
    
    f_sl_script.write(header)
    f_sl_script.write(txt)
    f_sl_script.close()
    
    fname_sl_abs_path = os.path.join(os.path.abspath(os.path.curdir),fname_slurm_script)
    print "Slurm script %s written" % fname_sl_abs_path
    generated_script = fname_sl_abs_path
    return generated_script 

if __name__ == '__main__':
    #Start of main function
    parser = argparse.ArgumentParser()
    parser.add_argument("--ncore",type=str,default=default_core)
    parser.add_argument("--auto", nargs="?", type=str,const=True)
    parser.add_argument('--account', type=str, default=default_account)
    parser.add_argument('--srf',type=str,default=None)
    parser.add_argument('--set_params_only', nargs="?",type=str,const=True)
    args = parser.parse_args()

    if args.ncore != default_core:
        #replace the default_core with ncore provided by userme_slurm_script
        default_core = args.ncore

    try:
        import params
    except:
        print "import params.py failed."
        sys.exit()
    else:
        wct_set=False 
        created_scripts = []
        if args.auto == True:
            submit_yes = True
        else:
            submit_yes = confirm("Also submit the job for you?")
        for srf in params.srf_files:
            #get the srf(rup) name without extensions
            srf_name = os.path.splitext(basename(srf))[0]
            #if srf(variation) is provided as args, only create the slurm with same name provided
            if args.srf != None and srf_name != args.srf:
                continue
            if args.set_params_only == True:
                set_runparams.create_run_parameters(srf_name)
                continue
            #get lf_sim_dir
            lf_sim_dir = os.path.join(params.lf_sim_root_dir,srf_name) 
            sim_dir = params.sim_dir
            nx = int(params.nx)
            ny = int(params.ny)
            nz = int(params.nz)
            dt = float(params.dt)
            sim_duration = float(params.sim_duration)
            #default_core will be changed is user pars ncore
            num_procs = default_core
            total_est_core_hours= est_e3d.est_core_hours_emod3d(nx,ny,nz,dt,sim_duration)
            estimated_wct = est_e3d.est_wct(total_est_core_hours,num_procs, default_wct_scale)
            print "Estimated WCT (scaled and rounded up):%s"%estimated_wct
            
            
            if args.auto == True:
                created_scripts = write_sl_script(lf_sim_dir,sim_dir,srf_name, run_time = estimated_wct)
                jobid = submit_sl_script(created_scripts,submit_yes)
            else:
                if wct_set == False:
                    wall_clock_limit = str(install.get_input_wc())
                    wct_set = True
                if wct_set == True:
                    print "WCT set to: %s"%wall_clock_limit
                created_scripts = write_sl_script(lf_sim_dir,sim_dir,srf_name,run_time = wall_clock_limit)
                jobid = submit_sl_script(created_scripts,submit_yes)
            #update the db if
            if jobid != None:
                try:
                    int(jobid)
                except:
                    print "error while parsing the jobid, please check the scipt"
                    sys.exit()
                #cmd = "python $gmsim/workflow/scripts/management/update_mgmt_db.py %s EMOD3D in-queue --run_name %s --j %s"%(params.mgmt_db_location, srf_name,jobid)
                #exe(cmd, debug=False)
                
                process = 'EMOD3D'
                status = 'in-queue'
                db = create_mgmt_db.connect_db(params.mgmt_db_location)
                update_mgmt_db.update_db(db, process, status, job=jobid, run_name=srf_name)
                db.connection.commit()
