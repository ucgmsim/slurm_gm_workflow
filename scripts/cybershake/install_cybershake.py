import os
import sys
import glob
import argparse

import shared_workflow.load_config as ldcfg
from qcore import utils

from qcore import validate_vm
from scripts import install
from scripts.management import create_mgmt_db
# from shared_workflow.shared import *
from shared_workflow.shared import verify_user_dirs

default_dt = 0.005
default_hf_dt = 0.005


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("path_cybershake", type=str, default=None,
                        help="the path to the root of a specific version cybershake.")
    parser.add_argument("config", type=str, default=None,
                        help="a path to a config file that constains all the required values.")
    parser.add_argument("vm", type=str, default=None, help="the name of the Velocity Model.")
    parser.add_argument('--version', type=str, default='16.1', help="version of simulation. eg.16.1")
    parser.add_argument("--n_rel", type=int, default=None, help="the number of realisations expected")

    args = parser.parse_args()
    try:
        # try to read the config file
        cybershake_cfg = ldcfg.load(os.path.dirname(args.config), os.path.basename(args.config))
    except:
        print("Error while parsing the config file, please double check inputs.")
        sys.exit()

    ldcfg.check_cfg_params_path(cybershake_cfg, 'dt', 'hf_dt')

    path_cybershake = args.path_cybershake

    sim_root_dir = os.path.join(path_cybershake, 'Runs/')

    srf_root_dir = os.path.join(path_cybershake, 'Data/Sources/')
    vm_root_dir = os.path.join(path_cybershake, 'Data/VMs/')

    global_root = cybershake_cfg['global_root']
    # this variable seems to not be used anywhere important.
    run_dir = sim_root_dir
    user_root = os.path.join(run_dir, 'Cybershake')
    stat_file_path = cybershake_cfg['stat_file_path']
    vs30_file_path = stat_file_path.replace('.ll', '.vs30')
    vs30ref_file_path = stat_file_path.replace('.ll', '.vs30ref')

    error_log = os.path.join(path_cybershake, "install_error.log")
    error_fp = open(error_log, 'a')
    params_vel = 'params_vel.py'

    source = args.vm

    # this variable has to be empty
    # TODO: fix this legacy issue, very low priority
    event_name = ""

    run_name = source
    vel_mod_dir = os.path.join(vm_root_dir, source)
    # print vel_mod_dir
    valid_vm, message = validate_vm.validate_vm(vel_mod_dir)
    if not valid_vm:
        message = "Error: VM {} failed {}".format(source, message)
        print message
        error_fp.write(message)
        exit()


    params_vel_path = os.path.join(vel_mod_dir, params_vel)

    execfile(params_vel_path, globals())

    yes_statcords = True  # always has to be true to get the fd_stat
    yes_model_params = False  # statgrid should normally be already generated with Velocity Model
    vel_mod_params_dir = vel_mod_dir

    # get all srf from source
    srf_dir = os.path.join(os.path.join(srf_root_dir, source), "Srf")
    stoch_dir = os.path.join(os.path.join(srf_root_dir, source), "Stoch")
    list_srf = glob.glob(os.path.join(srf_dir, '*.srf'))
    if args.n_rel is not None and len(list_srf) != args.n_rel:
        message = "Error: fault {} failed. Number of realisations do not match number of SRF files".format(source)
        print message
        error_fp.write(message)
        sys.exit()
    
    fault_yaml_path = os.path.join(sim_root_dir, 'fault_params.yaml')
    root_yaml_path = os.path.join(path_cybershake, 'root_params.yaml')
    for srf in list_srf:
        # try to match find the stoch with same basename
        srf_name = os.path.splitext(os.path.basename(srf))[0]
        stoch_file_path = os.path.join(stoch_dir, srf_name + '.stoch')
        if not os.path.isfile(stoch_file_path):
            message = "Error: Corresponding Stoch file is not found:\n{}".format(stoch_file_path)
            print message
            error_fp.write(message)
            sys.exit()
        else:
            # install pairs one by one to fit the new structure
            sim_dir = os.path.join(os.path.join(sim_root_dir, source), srf_name)
            root_params_dict, fault_params_dict, sim_params_dict, vm_params_dict = install.action(args.version, sim_dir, event_name, run_name, run_dir, vel_mod_dir, srf_dir, srf, stoch_file_path,
                       params_vel_path, stat_file_path, vs30_file_path, vs30ref_file_path, MODEL_LAT, MODEL_LON,
                       MODEL_ROT, hh, nx, ny, nz, sufx, sim_duration, vel_mod_params_dir, yes_statcords,
                       yes_model_params, fault_yaml_path, root_yaml_path)

            create_mgmt_db.create_mgmt_db([], path_cybershake, srf_files=srf)
            utils.setup_dir(os.path.join(path_cybershake, 'mgmt_db_queue'))
            root_params_dict['mgmt_db_location'] = path_cybershake
            # store extra params provided
            if 'hf_stat_vs_ref' in cybershake_cfg:
                root_params_dict['hf_stat_vs_ref'] = cybershake_cfg['hf_stat_vs_ref']

            install.dump_all_yamls(sim_dir, root_params_dict, fault_params_dict, sim_params_dict, vm_params_dict)

            # make symbolic link after install sim folder
            # cmd = "ln -s %s %s" % (srf_root_dir, os.path.join(sim_dir, 'Src'))
            # print cmd


if __name__ == '__main__':
    main()

