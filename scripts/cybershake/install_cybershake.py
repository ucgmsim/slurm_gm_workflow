import os
import sys
import glob
import argparse

import shared_workflow.load_config as ldcfg
from qcore import utils

from qcore import validate_vm, simulation_structure
from scripts import install
from scripts.management import create_mgmt_db
from shared_workflow import shared_defaults


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("path_cybershake", type=str, default=None,
                        help="the path to the root of a specific version cybershake.")
    parser.add_argument("config", type=str, default=None,
                        help="a path to a config file that constains all the required values.")
    parser.add_argument("vm", type=str, default=None, help="the name of the Velocity Model.")
    parser.add_argument("--n_rel", type=int, default=None, help="the number of realisations expected")

    args = parser.parse_args()
    try:
        # try to read the config file
        cybershake_cfg = ldcfg.load(os.path.dirname(args.config), os.path.basename(args.config))
    except:
        print("Error while parsing the config file, please double check inputs.")
        sys.exit()

    ldcfg.check_cfg_params_path(cybershake_cfg, 'dt', 'hf_dt', 'version')

    path_cybershake = args.path_cybershake

    sim_root_dir = os.path.join(path_cybershake, 'Runs/')

    #srf_root_dir = os.path.join(path_cybershake, 'Data/Sources/')
    #vm_root_dir = os.path.join(path_cybershake, 'Data/VMs/')

    global_root = cybershake_cfg['global_root']
    version = cybershake_cfg['version']
    v1d_full_path = cybershake_cfg['v_1d_mod']

    site_v1d_dir = None
    hf_stat_vs_ref = None
    if cybershake_cfg.get('site_v1d_dir') is not None:
        site_v1d_dir = cybershake_cfg['site_v1d_dir']
    if cybershake_cfg.get('hf_stat_vs_ref') is not None:
        hf_stat_vs_ref = cybershake_cfg['hf_stat_vs_ref']

    # this variable seems to not be used anywhere important.
    run_dir = sim_root_dir
    user_root = os.path.join(run_dir, 'Cybershake')
    stat_file_path = cybershake_cfg['stat_file_path']
    vs30_file_path = stat_file_path.replace('.ll', '.vs30')
    vs30ref_file_path = stat_file_path.replace('.ll', '.vs30ref')

    error_log = os.path.join(path_cybershake, "install_error.log")
    params_vel = 'params_vel.py'

    source = args.vm

    # this variable has to be empty
    # TODO: fix this legacy issue, very low priority
    event_name = ""

    #vel_mod_dir = os.path.join(vm_root_dir, source)
    vel_mod_dir = simulation_structure.get_VM_dir(path_cybershake, source)
    # print vel_mod_dir
    valid_vm, message = validate_vm.validate_vm(vel_mod_dir)
    if not valid_vm:
        message = "Error: VM {} failed {}".format(source, message)
        print(message)
        with open(error_log, 'a') as error_fp:
            error_fp.write(message)
        exit()

    params_vel_path = os.path.join(vel_mod_dir, params_vel)

    exec(open(params_vel_path).read(), globals())

    yes_statcords = True  # always has to be true to get the fd_stat
    yes_model_params = False  # statgrid should normally be already generated with Velocity Model
    #vel_mod_params_dir = vel_mod_dir

    # get all srf from source
    #list_srf = glob.glob(os.path.join(srf_root_dir, source, "Srf", '*.srf'))
    list_srf = glob.glob(os.path.join(path_cybershake, 'Data', 'Sources', source, "Srf", '*.srf'))
    if args.n_rel is not None and len(list_srf) != args.n_rel:
        message = "Error: fault {} failed. Number of realisations do not match number of SRF files".format(source)
        print(message)
        with open(error_log, 'a') as error_fp:
            error_fp.write(message)
        sys.exit()
    
    fault_yaml_path = os.path.join(sim_root_dir, 'fault_params.yaml')
    root_yaml_path = os.path.join(path_cybershake, 'root_params.yaml')
    for srf in list_srf:
        # try to match find the stoch with same basename
        realisation = os.path.splitext(os.path.basename(srf))[0]
        stoch_file_path = simulation_structure.get_stoch_path(path_cybershake, realisation)
        sim_params_file = simulation_structure.get_source_params_path(path_cybershake, realisation)
        if not os.path.isfile(stoch_file_path):
            message = "Error: Corresponding Stoch file is not found:\n{}".format(stoch_file_path)
            print(message)
            with open(error_log, 'a') as error_fp:
                error_fp.write(message)
            sys.exit()
        else:
            # install pairs one by one to fit the new structure
            sim_dir = os.path.join(sim_root_dir, source, realisation)
            root_params_dict, fault_params_dict, sim_params_dict, vm_params_dict = install.action(
                version, sim_dir, event_name, source, run_dir, vel_mod_dir, srf, stoch_file_path, params_vel_path,
                stat_file_path, vs30_file_path, vs30ref_file_path, MODEL_LAT, MODEL_LON, MODEL_ROT, hh, nx, ny, nz,
                sufx, sim_duration, vel_mod_dir, yes_statcords, yes_model_params, fault_yaml_path,
                root_yaml_path,  user_root=user_root, site_v1d_dir=site_v1d_dir, hf_stat_vs_ref=hf_stat_vs_ref,
                v1d_full_path=v1d_full_path,sim_params_file=sim_params_file)

            create_mgmt_db.create_mgmt_db([], path_cybershake, srf_files=srf)
            utils.setup_dir(os.path.join(path_cybershake, 'mgmt_db_queue'))
            root_params_dict['mgmt_db_location'] = path_cybershake
            # store extra params provided. This step has been moved into install_bb function
            # if 'hf_stat_vs_ref' in cybershake_cfg:
            #     root_params_dict['hf_stat_vs_ref'] = cybershake_cfg['hf_stat_vs_ref']

            install.dump_all_yamls(sim_dir, root_params_dict, fault_params_dict, sim_params_dict, vm_params_dict)

            # make symbolic link after install sim folder
            # cmd = "ln -s %s %s" % (srf_root_dir, os.path.join(sim_dir, 'Src'))
            # print cmd


if __name__ == '__main__':
    main()

