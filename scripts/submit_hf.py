
import glob
import os.path
import sys

sys.path.append(os.path.abspath(os.path.curdir))
from params import *
from params_base_bb import *
import fnmatch
# TODO: move this to qcore library
from temp_shared import resolve_header
from shared import *

def confirm(q):
    show_horizontal_line()
    print q
    return show_yes_no_question()


# TODO: implement submit_sl_script
def submit_sl_script(script_name):
    print "Submitting is not implemented yet!"



def write_sl_script(hf_dir, sl_template_prefix, hf_option):
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

        fname_sl_script = '%s_%s.sl' % (sl_template_prefix, variation)
        f_llscript = open(fname_sl_script, 'w')
        # TODO: change this values to values that make more sense or come from somewhere
        nb_cpus = "24"
        run_time = "00:30:00"
        job_name = "sim_hf_%s" % variation
        memory = "16G"
        header = resolve_header("nesi00213", nb_cpus, run_time, job_name, "slurm", memory,
                                job_description="HF calculation", additional_lines="#SBATCH -C avx")
        f_llscript.write(header)
        f_llscript.write(txt)
        f_llscript.close()
        print "Slurm script %s written" % fname_sl_script
        generated_scripts.append(fname_sl_script)

    return generated_scripts


# TODO: this is legacy
version = 'SERIAL'
if len(sys.argv) == 2:
    version = sys.argv[1]
    if version == 'MPI':
        ll_name_prefix = 'run_hf_mpi'
    elif version == 'MP':
        ll_name_prefix = 'run_hf_mp'
    else:
        print '%s is an invalid option' % version
        version = 'SERIAL'
        print 'Set to default %s' % version
if version == 'SERIAL':
    ll_name_prefix = 'run_hf'

ll_name_prefix = "run_hf"

hf_option = 0
print version
try:
    if rand_reset:
        hf_option = 1
except:
    print "Note: rand_reset is not defined in params_base_bb.py. We assume rand_reset=%s"%bool(hf_option)
    pass
try:
    if site_specific:
        print "Note: site_specific = True, rand_reset = True"
        hf_option = 2
except:
    print "Note: site_specific is not defined in params_base_bb.py. Default 'False' is used"
    pass

created_scripts = write_sl_script(hf_dir, ll_name_prefix, hf_option)
submit_sl_script(created_scripts)
