# TODO: import the CONFIG here

import glob
import os.path
import sys
import os
import set_runparams

# TODO: this needs to append the path to qcore as well
qcore_path = '/projects/nesi00213/qcore'
sys.path.append(qcore_path)
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
#default_emod3d_coef=3.00097
#coef should be predefined in est_emod3d.py
default_ch_scale=1.1
default_wct_scale=1.2

# TODO: remove this once temp_shared is gone
from temp_shared import resolve_header

sys.path.append(os.getcwd())

print(sys.path)

import install

# section for parser to determine if using automate wct
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--auto", nargs="?", type=str,const=True)
args = parser.parse_args()


def confirm(q):
    show_horizontal_line()
    print q
    return show_yes_no_question()


def create_sl(submit_yes,wall_clock_limit):
    from params_base import tools_dir

    # execfile(os.path.join(bin_process_dir, "set_runparams.py"))
    set_runparams.create_run_parameters()

    glob.glob('LF/*')
    lf_sim_dirs = glob.glob('LF/*')
    f_template = open('run_emod3d.sl.template')
    template = f_template.readlines()
    str_template = ''.join(template)

    for lf_sim_dir in lf_sim_dirs:
        txt = str_template.replace("{{lf_sim_dir}}", lf_sim_dir).replace("{{tools_dir}}", tools_dir)
        rup_mod = lf_sim_dir.split('/')[1]
        fname_slurm_script = 'run_emod3d_%s.sl' % rup_mod
        f_sl_script = open(fname_slurm_script, 'w')

        # slurm header
        # TODO: change this values to values that make more sense
        # TODO: this value has to change accordingly to the value used for WCT estimation
        nb_cpus = default_core
        #run_time = "1:00:00"
        run_time = wall_clock_limit
        job_name = "emod3d_%s" % rup_mod
        memory=default_memory
        header = resolve_header("nesi00213", nb_cpus, run_time, job_name, "slurm", memory, exe_time,
                                job_description="emod3d slurm script", additional_lines="#SBATCH --hint=nomultithread")

        f_sl_script.write(header)
        f_sl_script.write(txt)
        f_sl_script.close()
        print "Slurm script %s written" % fname_slurm_script
        if submit_yes:
            # TODO: implement submit_sl_script function and use it here
            print "Submitting %s" % fname_slurm_script
            res = exe("sbatch %s" % fname_slurm_script, debug=False)
            # print res
        else:
            print "User chose to submit the job manually"


if args.auto == True:
    #enable this feature after wct is properly implemented
    #print "This feature is disabled for slurm scripts"
    #exit(1)

    # TODO: prepare an auto submit for slurm scripts
    # TODO: resume using WCT functions after wct is updated to current estimation method
    #import wct

    try:
        import params
    except:
        print "import params.py failed. check sys.path"
    else:
        # TODO: get some values for when DB is empty
        # TODO: resume the WCT functions after wct is updated
        #db = wct.WallClockDB('wallclock.sqlite')
        nx = int(params.nx)
        ny = int(params.ny)
        nz = int(params.nz)
        dt = float(params.dt)
        sim_duration = float(params.sim_duration)
        # TODO: decide if the nproc should be defined in params.py or parsed( or both?)
        # using defaut_core for now, update this ASAP
        #num_procs = int(params.n_proc)
        num_procs = default_core
        #TODO: resume these functions when WCT properly implemented
        #est = db.estimate_wall_clock_time(nx, ny, nz, sim_duration, num_procs)
        #est_max = est[0]
        # all_clock_limit = est_max
        total_est_core_hours= est_e3d.est_cour_hours_emod3d(nx,ny,nz,dt,sim_duration)
        estimated_wct = est_e3d.est_wct(total_est_core_hours,num_procs, default_wct_scale)
        submit_yes=True
        create_sl(submit_yes,estimated_wct)
else:

    # install.wallclocl returns a datetime type value, transform it into string
    #wall_clock_limit = str(install.wallclock('.'))
    
    #TODO: replace all the WCT estimation with proper modules after wct.py is updated
    try:
        import params
    except:
        print "import params.py failed. check sys.path"
    else:
        nx = int(params.nx)
        ny = int(params.ny)
        nz = int(params.nz)
        dt = float(params.dt)
        sim_duration = float(params.sim_duration)
        num_procs = default_core
        total_est_core_hours= est_e3d.est_cour_hours_emod3d(nx,ny,nz,dt,sim_duration)
        estimated_wct = est_e3d.est_wct(total_est_core_hours,num_procs, default_wct_scale)
        print "Estimated WCT (scaled and rounded up):%s"%estimated_wct
        wall_clock_limit = str(install.get_input_wc())
        
        submit_yes = confirm("Also submit the job for you?")

        create_sl(submit_yes,wall_clock_limit)
