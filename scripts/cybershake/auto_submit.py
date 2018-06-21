from scripts.management import slurm_query_status
from scripts.management import create_mgmt_db
from scripts.management import update_mgmt_db
from subprocess import call

import shared_workflow.load_config as ldcfg

import argparse
import os
import sys

default_binary_mode = True
default_n_runs = 10 
default_1d_mod = "/nesi/transit/nesi00213/VelocityModel/Mod-1D/Cant1D_v2-midQ_leer.1d"
default_hf_vs30_ref = None

def submit_task(sim_dir, proc_type, run_name,db,binary_mode=True):
    #TODO: using shell call is EXTREMELY undesirable. fix this in near future(fundamentally)
    #change the working directory to the sim_dir
    os.chdir(sim_dir)
#    print "sim_dir:%s"%sim_dir
    #idenfity the proc_type, EMOD3D:1, merge_ts:2, winbin_aio:3, HF:4, BB:5
    if proc_type == 1:
        #EMOD 3D
        lf_sim_dir = os.path.join(sim_dir,"LF/%s"%run_name)
        if not os.path.isfile(os.path.join(lf_sim_dir,"params_uncertain.py")):
            print os.path.join(lf_sim_dir,"params_uncertain.py")," missing, creating"
            call("python $gmsim/workflow/scripts/submit_emod3d.py --set_params_only", shell=True)
            print "python $gmsim/workflow/scripts/submit_emod3d.py --set_params_only"
        print "python $gmsim/workflow/scripts/submit_emod3d.py --auto --srf %s"%run_name
        call("python $gmsim/workflow/scripts/submit_emod3d.py --auto --srf %s"%run_name, shell=True)
    if proc_type == 2:
        print "python $gmsim/workflow/scripts/submit_post_emod3d.py --auto --merge_ts --srf %s"%run_name
        call("python $gmsim/workflow/scripts/submit_post_emod3d.py --auto --merge_ts --srf %s"%run_name, shell=True)
    if proc_type == 3:
        #skipping winbin_aio if running binary mode
        if binary_mode == True:
            #update the mgmt_db
            update_mgmt_db.update_db(db,'winbin_aio','completed',run_name=run_name)    
        else:
            print "python $gmsim/workflow/scripts/submit_post_emod3d.py --auto --winbin_aio --srf %s"%run_name
            call("python $gmsim/workflow/scripts/submit_post_emod3d.py --auto --winbin_aio --srf %s"%run_name, shell=True)
    if proc_type == 4:
        #run the submit_post_emod3d before install_bb and submit_hf
        #TODO: fix this strange logic in the actual workflow
        #see if params_uncertain.py exsist
        if not os.path.isfile(os.path.join(sim_dir,"params_base_bb.py")):
            if default_hf_vs30_ref != None:
                print "python $gmsim/workflow/scripts/install_bb.py --v1d %s --hf_stat_vs_ref %s"%(default_1d_mod,default_hf_vs30_ref)
                call("python $gmsim/workflow/scripts/install_bb.py --v1d %s --hf_stat_vs_ref %s"%(default_1d_mod,default_hf_vs30_ref), shell=True)
            else:
                print "python $gmsim/workflow/scripts/install_bb.py --v1d %s"%default_1d_mod
                call("python $gmsim/workflow/scripts/install_bb.py --v1d %s"%default_1d_mod, shell=True)
        print "python $gmsim/workflow/scripts/submit_hf.py --binary --auto --srf %s"%run_name
        call("python $gmsim/workflow/scripts/submit_hf.py --binary --auto --srf %s"%run_name, shell=True)
    if proc_type == 5:
        print "python $gmsim/workflow/scripts/submit_bb.py --binary --auto --srf %s"%run_name
        call("python $gmsim/workflow/scripts/submit_bb.py --binary --auto --srf %s"%run_name, shell=True)
                
    
    

def get_vmname(srf_name):
    '''
        this function is mainly used for cybershake perpose
        get vm name from srf
        can be removed if mgmt_DB is updated to store vm name
    '''
    vm_name = srf_name.split('_')[0]
    return vm_name

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('run_folder', type=str, help="folder to the collection of runs on Kupe")
    parser.add_argument('--n_runs', default=default_n_runs, type=int)
    #cybershake-like simulations store mgmnt_db at different locations
    parser.add_argument('--single_sim', nargs="?", type=str,const=True)
    parser.add_argument('--config',type=str,default=None,help="a path to a config file that constains all the required values.")
    parser.add_argument('--no_im', action="store_true")

    args = parser.parse_args()
    mgmt_db_location = args.run_folder
    n_runs_max = args.n_runs
    db = create_mgmt_db.connect_db(mgmt_db_location)
    db_tasks = []
   
    if args.config != None: 
        #parse and check for variables in config
        try:
            print "!!!!!!!!!!!!!",args.config
            qcore_cfg = ldcfg.load(directory=os.path.dirname(args.config),cfg_name=os.path.basename(args.config))
            print qcore_cfg
        except Exception as e:
            print e
            print "Error while parsing the config file, please double check inputs."
            sys.exit()
        if 'v_1d_mod' in qcore_cfg:
            #TODO:bad hack, fix this when possible (with parsing)
            global default_1d_mod
            default_1d_mod = qcore_cfg['v_1d_mod']
        if 'hf_stat_vs_ref' in qcore_cfg:
            #TODO:bad hack, fix this when possible (with parsing)
            global default_hf_vs30_ref
            default_hf_vs30_ref =  qcore_cfg['hf_stat_vs_ref']
        if 'binary_mode' in qcore_cfg:
            binary_mode = qcore_cfg['binary_mode']
        else:
            binary_mode = default_binary_mode
        #append more logic here if more variables are requested 


    queued_tasks = slurm_query_status.get_queued_tasks()
    db_tasks = slurm_query_status.get_submitted_db_tasks(db)
    slurm_query_status.update_tasks(db, queued_tasks, db_tasks)
    db_tasks = slurm_query_status.get_submitted_db_tasks(db)
    #submitted_tasks = slurm_query_status.get_submitted_db_tasks(db)
    runnable_tasks = slurm_query_status.get_runnable_tasks(db)
    
    ntask_to_run = n_runs_max - len(db_tasks)
    submit_task_count = 0
    task_num=0
    print submit_task_count
    print ntask_to_run
    while submit_task_count != ntask_to_run and submit_task_count < len(runnable_tasks):
        db_task_status = runnable_tasks[task_num]
        
        proc_type = db_task_status[0]
        run_name = db_task_status[1]
        task_state = db_task_status[2]

        #skip im calcs if no_im == true
        if args.no_im and proc_type == 6:
            task_num = task_num + 1
            continue
        

        vm_name = get_vmname(run_name)

        if args.single_sim == True:
            #TODO: if the directory changed, this may break. make this more robust
            sim_dir = mgmt_db_location 
        else:
            #non-cybershake, db is the same loc as sim_dir
            sim_dir = os.path.join(os.path.join(mgmt_db_location,"Runs"), vm_name)
        #submit the job
        submit_task(sim_dir, proc_type, run_name, db, binary_mode)
       
        submit_task_count = submit_task_count + 1
        task_num = task_num + 1
        


if __name__ == '__main__':
   main() 




