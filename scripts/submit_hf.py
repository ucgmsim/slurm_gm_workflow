
import glob
import os.path
import sys

sys.path.append(os.path.abspath(os.path.curdir))
import params 
import params_base_bb 

import fnmatch

import argparse

#default values
default_version='run_hf_mpi'
default_core="80"
default_run_time="00:30:00"
default_memory="16G"
#datetime related
from datetime import datetime 
timestamp_format = "%Y%m%d_%H%M%S"
timestamp = datetime.now().strftime(timestamp_format)



# TODO: move this to qcore library
from temp_shared import resolve_header
from qcore.shared import *

def confirm(q):
    show_horizontal_line()
    print q
    return show_yes_no_question()


# TODO: implement submit_sl_script
def submit_sl_script(script_names):
    #print "Submitting is not implemented yet!"
    submit_yes = confirm("Also submit the job for you?")
    if submit_yes:
        for script in script_names:
            print "Submitting %s" %script
            res=exe("sbatch %s"%script,debug=False)
    else:
        print "User chose to submit the job manually"
    



def write_sl_script(hf_dir, sl_template_prefix, hf_option, nb_cpus=default_core, run_time=default_run_time,memory=default_memory):
    hf_sim_dirs = []
    file_to_find = 'params_bb_uncertain.py'
    for root, dirnames, filenames in os.walk(hf_dir):
        for filename in fnmatch.filter(filenames, file_to_find):
            hf_sim_dirs.append(root)
    print hf_sim_dirs
    f_template = open('%s.sl.template' % sl_template_prefix)
    template = f_template.readlines()
    str_template = ''.join(template)
    generated_scripts = []
    for hf_sim_dir in hf_sim_dirs:
        txt = str_template.replace("{{hf_sim_dir}}", hf_sim_dir)
        txt = txt.replace("{{hf_option}}",str(hf_option))
        variation = hf_sim_dir.replace(hf_dir + '/', '').replace('/', '__')
        print variation

        fname_sl_script = '%s_%s_%s.sl' % (sl_template_prefix, variation,timestamp)
        f_llscript = open(fname_sl_script, 'w')
        job_name = "sim_hf_%s" % variation

        header = resolve_header("nesi00213", nb_cpus, run_time, job_name, "slurm", memory, timestamp,
                                job_description="HF calculation", additional_lines="###SBATCH -C avx")
        f_llscript.write(header)
        f_llscript.write(txt)
        f_llscript.close()
        print "Slurm script %s written" % fname_sl_script
        generated_scripts.append(fname_sl_script)

    return generated_scripts


if __name__ == '__main__':
    #parse the arguments 
    parser = argparse.ArgumentParser()
    #if some reason user decide to use different version, instead of mpi
    parser.add_argument('--version',type=str,default=None,const=None)
    #optional args for ncore, WCT, etc.
    parser.add_argument('--ncore',type=str,default=default_core)
    parser.add_argument('--wct',type=str,nargs='?',default=None,const=None)
    #the const of auto is set to True, so that as long as --auto is used, no more value needs to be provided
    parser.add_argument('--auto',type=int,nargs='?',default=None,const=True)
    #rand_reset, if somehow the user decide to use it but not defined in params_base_bb
    #the const is set to True, so that as long as --rand_reset is used, no more value needs to be provided
    parser.add_argument('--rand_reset',type=int,nargs='?',default=None,const=True)
    parser.add_argument('--site_specific',type=int,nargs='?',default=None,const=True)
    args = parser.parse_args()
   

    #check if the args is none, if not, change the version
    if args.version != None:
        version = args.version
        if version == 'serial' or version == 'run_hf':
            ll_name_prefix = 'run_hf'
        elif version == 'mp' or version == 'run_hf_mp':
            ll_name_prefix = 'run_hf_mp'
        elif version == 'mpi' or version == 'run_hf_mpi':
            ll_name_prefix = 'run_hf_mpi'
        else:
            print '% cannot be recognize as a valide option'%version
            print 'version is set to default: %',default_version
            version = default_version
            ll_name_prefix = default_version
    else:
        version = default_version
        ll_name_prefix = default_version
    print "version:",version
    #if auto flag is set to true, auto estimate the WCT and use default cores(or get from --ncore)
    
    #check rand_reset
    if args.site_specific != None:
        print "Note: site_specific = True, rand_reset = True"
        hf_option = 2
    else:
        try:
            if args.rand_reset != None or params_base_bb.rand_reset:
                hf_option = 1
        except:
                hf_option = 0
                print "Note: rand_reset is not defined in params_base_bb.py. We assume rand_reset=%s"%bool(hf_option)
        
    #check if parsed ncore
    if args.ncore != default_core:
        ncore = args.ncore
    else:
        ncore = default_core

    #TODO: add in wct estimation
    run_time=default_run_time

    #--auto used, automatically assign run_time using estimation 

    #run the standard process(asking user), if --auto not used
    created_scripts = write_sl_script(params.hf_dir, ll_name_prefix, hf_option,ncore,run_time)
    submit_sl_script(created_scripts)
