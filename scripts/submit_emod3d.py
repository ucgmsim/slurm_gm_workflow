# TODO: import the CONFIG here

import glob
import os.path
from os.path import basename
import sys
import os
import set_runparams

#sys.path.append(os.path.abspath(os.path.curdir))

from qcore.shared import *
import estimate_emod3d as est_e3d

#datetime related
import datetime as dtl
exetime_pattern = "%Y%m%d_%H%M%S"
exe_time = dtl.datetime.now().strftime(exetime_pattern)

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

#sys.path.append(os.getcwd())

print(sys.path)

import install

# section for parser to determine if using automate wct
import argparse




def confirm(q):
    show_horizontal_line()
    print q
    return show_yes_no_question()

def submit_sl_script(script_names,submit_yes=None):
    #print "Submitting is not implemented yet!"
    if submit_yes == None:
        submit_yes = confirm("Also submit the job for you?")
    if submit_yes:
        for script in script_names:
            #encode if it is unicode
            #TODO:fix this in qcore.shared.exe()
            if type(script) == unicode:
                script = script.encode() 
            print "Submitting %s" %script
            res = exe("sbatch %s"%script, debug=False)
    else:
        print "User chose to submit the job manually"



def write_sl_script(lf_sim_dir,srf_name,wall_clock_limit, nb_cpus=default_core, run_time=default_run_time,memory=default_memory,account=default_account):

    from params_base import tools_dir

    set_runparams.create_run_parameters(srf_name)
    generated_scripts = []

    f_template = open('run_emod3d.sl.template')
    template = f_template.readlines()
    str_template = ''.join(template)
    
    txt = str_template.replace("{{lf_sim_dir}}", lf_sim_dir).replace("{{tools_dir}}", tools_dir)
    fname_slurm_script = 'run_emod3d_%s.sl' % srf_name
    f_sl_script = open(fname_slurm_script, 'w')

    # slurm header
    # TODO: this value has to change accordingly to the value used for WCT estimation
    job_name = "run_emod3d.%s" % srf_name
    header = resolve_header(args.account, nb_cpus, run_time, job_name, "slurm", memory, exe_time,
                            job_description="emod3d slurm script", additional_lines="#SBATCH --hint=nomultithread")
    
    f_sl_script.write(header)
    f_sl_script.write(txt)
    f_sl_script.close()
    
    fname_sl_abs_path = os.path.join(os.path.abspath(os.path.curdir),fname_slurm_script)
    print "Slurm script %s written" % fname_sl_abs_path
    generated_scripts.append(fname_sl_abs_path)
    return generated_scripts 

if __name__ == '__main__':
    #Start of main function
    parser = argparse.ArgumentParser()
    parser.add_argument("--ncore",type=str,default=default_core)
    parser.add_argument("--auto", nargs="?", type=str,const=True)
    parser.add_argument('--account', type=str, default=default_account)
    parser.add_argument('--srf',type=str,default=None)
    args = parser.parse_args()

    if args.ncore != default_core:
        #replace the default_core with ncore provided by user
        default_core = args.ncore

    try:
        import params
    except:
        print "import params.py failed."
        sys.exit()
    else:
        wct_set=False 
        for srf in params.srf_files:
            #get the srf(rup) name without extensions
            srf_name = os.path.splitext(basename(srf))[0]
            #if srf(variation) is provided as args, only create the slurm with same name provided
            if args.srf != None and srf_name != args.srf:
                continue
            #get lf_sim_dir
            lf_sim_dir = os.path.join(params.lf_sim_root_dir,srf_name) 
            # TODO: resume the WCT functions after wct is updated.
            # note: the wct funcitons is should be ran in the est_wct scripts
            nx = int(params.nx)
            ny = int(params.ny)
            nz = int(params.nz)
            dt = float(params.dt)
            sim_duration = float(params.sim_duration)
            #default_core will be changed is user pars ncore
            num_procs = default_core
            total_est_core_hours= est_e3d.est_cour_hours_emod3d(nx,ny,nz,dt,sim_duration)
            estimated_wct = est_e3d.est_wct(total_est_core_hours,num_procs, default_wct_scale)
            print "Estimated WCT (scaled and rounded up):%s"%estimated_wct
            
            if args.auto == True:
                created_scripts = write_sl_script(lf_sim_dir,srf_name,estimated_wct)
                submit_sl_script(created_scripts,submit_yes=True)
            else:
                if wct_set == False:
                    wall_clock_limit = str(install.get_input_wc())
                    wct_set = True
                created_scripts = write_sl_script(lf_sim_dir,srf_name,wall_clock_limit)
                submit_sl_script(created_scripts)
