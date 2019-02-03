import os
import getpass
from shared_workflow import load_config as ldcfg
import scripts
from qcore import config

workflow_config = ldcfg.load(os.path.dirname(os.path.realpath(scripts.__file__)), "workflow_config.json")
workflow_root = workflow_config['gm_sim_workflow_root']
global_root = workflow_config["global_root"]
bin_process_dir = os.path.join(global_root, 'workflow/scripts')
emod3d_version = workflow_config["emod3d_version"]
params_vel = workflow_config['params_vel']

run_dir = workflow_config['runfolder_path']
user = getpass.getuser()
user_root = os.path.join(run_dir, user)  # global_root
srf_default_dir = os.path.join(global_root, 'RupModel')
vel_mod_dir = os.path.join(global_root, 'VelocityModel')
recipe_dir = workflow_config['templates_dir']
v_mod_1d_dir = os.path.join(global_root, 'VelocityModel', 'Mod-1D')
stat_dir = os.path.join(global_root, 'StationInfo')
#tools_dir = config.get_tools_dir('emod3d', '3.0.4-gcc')
latest_ll_dir = os.path.join(global_root, 'StationInfo/grid')
latest_ll = 'non_uniform_with_real_stations_latest'

# TODO: after enabling different dt for LF and HF, this might need to change
# the following dts are now set in templates/gmsim/version/*_defaults.yaml
# default_dt = 0.005
# default_hf_dt = 0.005

