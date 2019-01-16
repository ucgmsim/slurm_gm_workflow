import os
import sys
import glob
import shared_workflow.load_config as ldcfg

import argparse

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
    parser.add_argument("--n_rel", type=int, default=None, help="the number of realisations expected")

    args = parser.parse_args()
    try:
        # try to read the config file
        qcore_cfg = ldcfg.load(os.path.dirname(args.config), os.path.basename(args.config))
    except:
        print "Error while parsing the config file, please double check inputs."
        sys.exit()

    path_cybershake = args.path_cybershake

    sim_root_dir = os.path.join(path_cybershake, 'Runs/')

    srf_root_dir = os.path.join(path_cybershake, 'Data/Sources/')
    vm_root_dir = os.path.join(path_cybershake, 'Data/VMs/')

    global_root = qcore_cfg['global_root']
    # this variable seems to not be used anywhere important.
    run_dir = sim_root_dir
    user_root = os.path.join(run_dir, 'Cybershake')
    stat_file_path = qcore_cfg['stat_file_path']
    vs30_file_path = stat_file_path.replace('.ll', '.vs30')
    vs30ref_file_path = stat_file_path.replace('.ll', '.vs30ref')

    error_log = os.path.join(path_cybershake, "install_error.log")
    error_fp = open(error_log, 'a')
    params_vel = 'params_vel.py'

    # vars
    if 'dt' in qcore_cfg:
        dt = qcore_cfg['dt']
    else:
        dt = default_dt

    if 'hf_dt' in qcore_cfg:
        hf_dt = qcore_cfg['hf_dt']
    else:
        hf_dt = default_hf_dt

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
            print "!!!!SIM_DIR:%s" % sim_dir
            srf_files = []
            stoch_files = []
            srf_files.append(srf)
            stoch_files.append(stoch_file_path)

            srf_stoch_pairs = zip(srf_files, stoch_files)
            # print srf_stoch_pairs
            # print list_srf

            install.action(sim_dir, event_name, run_name, run_dir, vel_mod_dir, srf_root_dir, srf_stoch_pairs,
                           params_vel_path, stat_file_path, vs30_file_path, vs30ref_file_path, MODEL_LAT, MODEL_LON,
                           MODEL_ROT, hh, nx, ny, nz, sufx, sim_duration, flo, vel_mod_params_dir, yes_statcords,
                           yes_model_params, dt)

            create_mgmt_db.create_mgmt_db([], path_cybershake, srf_files=srf_files)
            with open(os.path.join(sim_dir, "params_base.py"), "a") as f:
                f.write("mgmt_db_location='%s'\n" % path_cybershake)

            # store extra params provided
            if 'hf_stat_vs_ref' in qcore_cfg:
                with open(os.path.join(sim_dir, "params_base.py"), "a") as f:
                    f.write("hf_stat_vs_ref='%s'\n" % qcore_cfg['hf_stat_vs_ref'])

            # remove old params_base.pyc
            try:
                print "Removing probably incomplete " + os.path.join(sim_dir, "params_base.pyc")
                os.remove(os.path.join(sim_dir, "params_base.pyc"))
            except Exception, e:
                print e.args
                print "Could not remove params_base.pyc"

            # make symbolic link after install sim folder
            cmd = "ln -s %s %s" % (srf_root_dir, os.path.join(sim_dir, 'Src'))
            # print cmd


if __name__ == '__main__':
    main()