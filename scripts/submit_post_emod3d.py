
import glob
import os.path
import sys
import math

# TODO: remove this once temp_shared is gone
from temp_shared import resolve_header

from qcore.shared import *

sys.path.append(os.getcwd())
from params_base import tools_dir

#datetime related
import datetime as dtl
exetime_pattern = "%Y%m%d_%H%M%S"
exe_time = dtl.datetime.now().strftime(exetime_pattern)


# TODO: hardcoding is bad!
# TODO: this number has to be extactly the same with EMOD3D(because of how we manage mpi in winbin-aio currently
# may no longer be so after an proper update on winbin-aio-mpi
max_tasks_per_node = "80"

def get_seis_len(seis_path):
    filepattern = os.path.join(seis_path, '*_seis*.e3d')
    seis_file_list = sorted(glob(filepattern))
    return len(seis_file_list)

def confirm(q):
    show_horizontal_line
    print q
    return show_yes_no_question()

merge_ts_name_prefix = "post_emod3d_merge_ts"
winbin_aio_name_prefix = "post_emod3d_winbin_aio"

glob.glob('LF/*')
lf_sim_dirs = glob.glob('LF/*')
print lf_sim_dirs
# reading merge_ts_template
merge_ts_template = open('%s.sl.template' % merge_ts_name_prefix)
merge_ts_template_contents = merge_ts_template.readlines()
merge_ts_str_template = ''.join(merge_ts_template_contents)
# reading winbin_aio_template
winbin_aio_template = open('%s.sl.template' % winbin_aio_name_prefix)
winbin_aio_template_contents = winbin_aio_template.readlines()
winbin_aio_str_template = ''.join(winbin_aio_template_contents)

submit_yes = confirm("Also submit the job for you?")

for lf_sim_dir in lf_sim_dirs:
    print "Working on", lf_sim_dir
    # preparing merge_ts submit
    txt = merge_ts_str_template.replace("{{lf_sim_dir}}", lf_sim_dir)
    try:
        txt = txt.replace("{{tools_dir}}", tools_dir)
    except:
        print "**error while replacing tools_dir**"

    outbin = os.path.join(lf_sim_dir, 'OutBin')
    seis_files = glob.glob(os.path.join(outbin, '*seis*.e3d'))
    n_seis = len(seis_files)

    rup_mod = lf_sim_dir.split('/')[1]

    # TODO: change this values to values that make more sense
    nb_cpus = "4"
    run_time = "00:30:00"
    job_name = "post_emod3d.merge_ts.%s" % rup_mod
    memory = "16G"
    header = resolve_header("nesi00213", nb_cpus, run_time, job_name, "Slurm", memory,exe_time,
                            job_description="post emod3d: merge_ts", additional_lines="###SBATCH -C avx")

    fname_merge_ts_script = '%s_%s.sl' % (merge_ts_name_prefix, rup_mod)
    final_merge_ts = open(fname_merge_ts_script, 'w')
    final_merge_ts.write(header)
    final_merge_ts.write(txt)
    final_merge_ts.close()
    print "Slurm script %s written" % fname_merge_ts_script

    # preparing winbin_aio
    txt = winbin_aio_str_template.replace("{{lf_sim_dir}}", lf_sim_dir)

    #get the file count of seis files
    path_outbin=os.path.join(lf_sim_dir,"OutBin")
    sfl_len=get_seis_len(path_outbin)
    #round down to the max cpu per node
    nodes = int(round( (sfl_len/max_tasks_per_node) - 0.5 ) )
    if nodes <= 0:
        nodes = 1
    nb_cpus = nodes*max_tasks_per_node
            


    # TODO: change this values to values that make more sense
    #nb_cpus = max_tasks_per_node 
    run_time = "00:30:00"
    job_name = "post_emod3d.winbin_aio.%s" % rup_mod
    memory = "16G"
    header = resolve_header("nesi00213", nb_cpus, run_time, job_name, "slurm", memory,exe_time,
                            job_description="post emod3d: winbin_aio", additional_lines="###SBATCH -C avx")

    fname_winbin_aio_script = '%s_%s.sl' % (winbin_aio_name_prefix, rup_mod)
    final_winbin_aio = open(fname_winbin_aio_script, 'w')
    final_winbin_aio.write(header)
    final_winbin_aio.write(txt)
    final_winbin_aio.close()
    print "Slurm script %s written" % fname_winbin_aio_script

    if submit_yes:
        # TODO: implement submit_sl_script and use here
        # print "Submitting not implemented yet!"
        res = exe("sbatch %s" % fname_merge_ts_script, debug=False)
        res = exe("sbatch %s" % fname_winbin_aio_script, debug=False)
    
        print res
    else:
        print "User chose to submit the job manually"
