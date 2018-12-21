import sys
import os
import json


global_root = sys.argv[1]
emod_version = "3.0.4"
gm_sim_root = os.path.join(global_root, "workflow")
shared_workflow_dir = os.path.join(global_root, "workflow/shared_workflow")
bin_process_path = os.path.join(global_root, "workflow/scripts")
qcore_path =  os.path.join(global_root, "qcore")
tools_path = os.path.join(global_root, "tools")

config_dictionary = {
  "gm_sim_workflow_root" : gm_sim_root,
  "emod3d_version" : "3.0.4",
  "global_root" : global_root,
  "bin_process_path" : bin_process_path,
  "qcore_lib_path" : qcore_path,
  "install_bb_name" : "install_bb.py",
  "params_vel" : "params_vel.py",
  "tools_dir" : "/nesi/project/nesi00213/opt/maui/emod3d/3.0.4-gcc/bin",
  "runfolder_path" : "/nesi/nobackup/nesi00213/RunFolder/"
}

with open(os.path.join(bin_process_path, "workflow_config.json"), "w") as f:
    json.dump(config_dictionary, f)

print "DONE"
