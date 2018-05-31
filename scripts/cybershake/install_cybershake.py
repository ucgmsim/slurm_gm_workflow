#this script will require an input for the "model" name.

import os
import sys
import glob
import qcore.load_config as ldcfg
qcore_cfg=ldcfg.load(os.path.join(os.path.dirname(os.path.realpath(__file__)),"cybershake_config.json"))

sys.path.insert(0,qcore_cfg['script_location'])
sys.path.insert(0,qcore_cfg['qcore_path'])


from scripts import install
from scripts.management import create_mgmt_db
from qcore.shared import *

if len(sys.argv) < 2:
    print "Usage: %s /path/to/cybershake/version"%sys.argv[0]
    sys.exit()

path_cybershake=sys.argv[1]

sim_root_dir=os.path.join(path_cybershake,'Runs/')

srf_root_dir=os.path.join(path_cybershake,'Data/Sources/')
vm_root_dir=os.path.join(path_cybershake,'Data/VMs/')


global_root = qcore_cfg['global_root']
run_dir = os.path.join(global_root,'RunFolder')
user_root = os.path.join(run_dir,'Cybershake')
stat_file_path= qcore_cfg['stat_file_path']
vs30_file_path = stat_file_path.replace('ll','vs30')
vs30ref_file_path = stat_file_path.replace('ll','vs30ref')

params_vel = 'params_vel.py'

#vars
dt = 0.02

#f=open('/home/ykh22/Cybershake/list_source.txt')
#
#source_list=[]
#
#for line in f:
#    source_list.append(line.replace('\n',''))

#print list

#waiting for input for model name, this will be handeled by pipe in the .sh script
source = raw_input()


event_name = ""
run_name = source
vel_mod_dir=os.path.join(vm_root_dir,source)
#print vel_mod_dir

#get all srf from source
srf_dir=os.path.join(os.path.join(srf_root_dir,source),"Srf")
list_srf= glob.glob(os.path.join(srf_dir,'*.srf'))
srf_files= []
stoch_files=[]
stoch_dir=os.path.join(os.path.join(srf_root_dir,source),"Stoch")
list_stoch= glob.glob(os.path.join(stoch_dir,'*.stoch'))

for srf in list_srf:
    srf_files.append(os.path.join(srf_dir,srf))

for stoch in list_stoch:
    stoch_files.append(os.path.join(stoch_dir,stoch))


srf_stoch_pairs = zip(srf_files, stoch_files)
#print srf_stoch_pairs
#print list_srf
params_vel_path = os.path.join(vel_mod_dir,params_vel)

execfile(params_vel_path,globals())

sim_dir = os.path.join(sim_root_dir,source)
print "!!!!SIM_DIR:%s"%sim_dir
yes_statcords = True #always has to be true to get the fd_stat
yes_model_params = False #statgrid should normally be already generated with Velocity Model
vel_mod_params_dir = vel_mod_dir

#srf_dir = srf_root_dir


install.action(sim_dir,event_name,run_name, run_dir, vel_mod_dir, srf_root_dir, srf_stoch_pairs,params_vel_path,stat_file_path, vs30_file_path, vs30ref_file_path, MODEL_LAT,MODEL_LON,MODEL_ROT,hh,nx,ny,nz,sufx,sim_duration,flo,vel_mod_params_dir,yes_statcords, yes_model_params, dt)

create_mgmt_db.create_mgmt_db([], path_cybershake, srf_files=srf_files)
with open(os.path.join(sim_dir,"params_base.py"),"a") as f:
    f.write("mgmt_db_location='%s'\n" % path_cybershake)
#remove old params_base.pyc
try:
    print "Removing probably incomplete "+os.path.join(sim_dir, "params_base.pyc")
    os.remove(os.path.join(sim_dir, "params_base.pyc"))
except Exception, e:
    print e.args
    print "Could not remove params_base.pyc"


#make symbolic link after install sim folder
cmd="ln -s %s %s"%(srf_root_dir, os.path.join(sim_dir,'Src'))
#print cmd
