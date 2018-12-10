#!/usr/bin/env python2

import os.path
import sys
import argparse
import glob

from qcore import utils
from shared_workflow import load_config
# TODO: make sure that qcore is in the PYTHONPATH
from shared_workflow.shared import *

sys.path.append(os.path.abspath(os.path.curdir))

params = utils.load_params('root_params.yaml', 'fault_params.yaml', 'sim_params.yaml')
utils.update(params, utils.load_params(os.path.join(params.vel_mod_dir, 'vm_params.yaml')))

workflow_config = load_config.load(os.path.dirname(os.path.realpath(__file__)), "workflow_config.json")
global_root = workflow_config["global_root"]
tools_dir = os.path.join(global_root, 'opt/maui/emod3d/3.0.4-gcc/bin')
emod3d_version = workflow_config["emod3d_version"]
V_MOD_1D_DIR = os.path.join(global_root, 'VelocityModel', 'Mod-1D')

old_params = True


def q0():
    show_horizontal_line()
    print "Do you want site-specific computation? (To use a universal 1D profile, Select 'No')"
    show_horizontal_line()
    return show_yes_no_question()


def q1_generic(v_mod_1d_dir):
    show_horizontal_line()
    print "Select one of 1D Velocity models (from %s)" % v_mod_1d_dir
    show_horizontal_line()

    v_mod_1d_options = glob.glob(os.path.join(v_mod_1d_dir, '*.1d'))
    v_mod_1d_options.sort()

    v_mod_1d_selected = show_multiple_choice(v_mod_1d_options)
    print v_mod_1d_selected  # full path
    v_mod_1d_name = os.path.basename(v_mod_1d_selected).replace('.1d', '')
    #    print v_mod_1d_name

    return v_mod_1d_name, v_mod_1d_selected


def q1_site_specific(stat_file_path, hf_stat_vs_ref=None, v1d_mod_dir=None):
    show_horizontal_line()
    print "Auto-detecting site-specific info"
    show_horizontal_line()
    print "- Station file path: %s" % stat_file_path

    if v1d_mod_dir != None:
        v_mod_1d_path = v1d_mod_dir
    else:
        v_mod_1d_path = os.path.join(os.path.dirname(stat_file_path), "1D")
    if os.path.exists(v_mod_1d_path):
        print "- 1D profiles found at %s" % v_mod_1d_path
    else:
        print "Error: No such path exists: %s" % v_mod_1d_path
        sys.exit()
    if hf_stat_vs_ref == None:
        hf_stat_vs_ref_options = glob.glob(os.path.join(stat_file_path, '*.hfvs30ref'))
        if len(hf_stat_vs_ref_options) == 0:
            print "Error: No HF Vsref file was found at %s" % stat_file_path
            sys.exit()
        hf_stat_vs_ref_options.sort()

        show_horizontal_line()
        print "Select one of HF Vsref files"
        show_horizontal_line()
        hf_stat_vs_ref_selected = show_multiple_choice(hf_stat_vs_ref_options)
        print " - HF Vsref tp be used: %s" % hf_stat_vs_ref_selected
    else:
        hf_stat_vs_ref_selected = hf_stat_vs_ref
    return v_mod_1d_path, hf_stat_vs_ref_selected


def q2(v_mod_1d_name, srf, kappa, sdrop):
    hf_sim_bin = os.path.join(tools_dir, 'hb_high_v5.4.5_np2mm+')
    hfVString = 'hf' + os.path.basename(hf_sim_bin).split('_')[-1]
    hf_run_name = v_mod_1d_name + '_' + hfVString + '_rvf' + str(params.hf.hf_rvfac) + '_sd' + str(sdrop) + '_k' + str(
        kappa)
    hf_run_name = hf_run_name.replace('.', 'p')
    show_horizontal_line()
    print "- Vel. Model 1D: %s" % v_mod_1d_name
    print "- hf_sim_bin: %s" % os.path.basename(hf_sim_bin)
    print "- hf_rvfac: %s" % params.hf.hf_rvfac
    print "- hf_sdrop: %s" % sdrop
    print "- hf_kappa: %s" % kappa
    print "- srf file: %s" % srf
    #    yes = confirm_name(hf_run_name)
    yes = True
    return yes, hf_run_name


def store_params(root_dict):
    f = open(os.path.join(params.sim_dir, "params_base_bb.py"), "w")
    keys = root_dict.keys()
    for k in keys:
        val = root_dict[k]
        if type(val) == str:
            val = "'%s'" % val
        f.write("%s=%s\n" % (k, val))
    f.close()


def action_for_uncertainties(hf_sim_basedir, bb_sim_basedir, srf, slip, kappa, sdrop):
    dirs = []
    srf_basename = os.path.splitext(os.path.basename(srf))[0]  # take the filename only
    hf_sim_dir = os.path.join(hf_sim_basedir, srf_basename)
    bb_sim_dir = os.path.join(bb_sim_basedir, srf_basename)
    dirs.append(hf_sim_dir)
    dirs.append(bb_sim_dir)

    verify_user_dirs(dirs)

    return hf_sim_dir, bb_sim_dir


def main():
    global hf_kappa_list  # no idea why hf_kappa_list is imported, but not usable in this function without this.
    global hf_sdrop_list

    parser = argparse.ArgumentParser()
    parser.add_argument('--v1d', default=None, type=str, help="the full path pointing to the generic v1d file")
    parser.add_argument('--site_v1d_dir', default=None, type=str,
                        help="the to the directory containing site specific files, hf_stat_vs_ref must be provied as well if this is provided")
    parser.add_argument('--hf_stat_vs_ref', default=None, type=str,
                        help="site_v1d_dir must be provied as well if this is provided")
    args = parser.parse_args()

    show_horizontal_line(c="*")
    print " " * 37 + "EMOD3D HF/BB Preparationi Ver." + params.bin_process_ver
    show_horizontal_line(c="*")

    root_dict = utils.load_yaml('root_params.yaml')
    root_dict['bb'] = {}
    # root_dict['rand_reset']=False #by default. But it may give less deterministic
    if args.v1d is not None:
        v_mod_1d_selected = args.v1d
        v_mod_1d_name = os.path.basename(v_mod_1d_selected).replace('.1d', '')
        root_dict['bb']['site_specific'] = False
        root_dict['v_mod_1d_name'] = v_mod_1d_selected
        # root_dict['hf_v_model']=v_mod_1d_selected
    # TODO:add in logic for site specific as well, if the user provided as args
    elif args.site_v1d_dir != None and args.hf_stat_vs_ref != None:
        v_mod_1d_path, hf_stat_vs_ref = q1_site_specific(os.path.dirname(params.stat_file),
                                                         hf_stat_vs_ref=args.hf_stat_vs_ref,
                                                         v1d_mod_dir=args.site_v1d_dir)
        v_mod_1d_name = "Site_Specific"
        root_dict['bb']['site_specific'] = True
        root_dict['v_mod_1d_name'] = v_mod_1d_path
        # root_dict['hf_v_model_path']=v_mod_1d_path
        root_dict['hf_stat_vs_ref'] = hf_stat_vs_ref
        root_dict['bb']['rand_reset'] = True
    else:
        is_site_specific_id = q0()
        if is_site_specific_id:
            v_mod_1d_path, hf_stat_vs_ref = q1_site_specific(os.path.dirname(params.stat_file))
            v_mod_1d_name = "Site_Specific"
            root_dict['bb']['site_specific'] = True
            root_dict['v_mod_1d_name'] = v_mod_1d_path
            # root_dict['hf_v_model_path']=v_mod_1d_path
            root_dict['hf_stat_vs_ref'] = hf_stat_vs_ref
            root_dict['bb']['rand_reset'] = True

        else:
            v_mod_1d_name, v_mod_1d_selected = q1_generic(V_MOD_1D_DIR)
            root_dict['bb']['site_specific'] = False
            root_dict['v_mod_1d_name'] = v_mod_1d_selected
            # root_dict['hf_v_model']=v_mod_1d_selected

    # root_dict['v_mod_1d_name']=v_mod_1d_name

    if old_params:
        hf_kappa_list = [params.hf.hf_kappa]
        hf_sdrop_list = [params.hf.hf_sdrop]
        if len(params.srf_file) > 1:
            print "Info: You have specified multiple SRF files."
            print "      A single hf_kappa(=%s) and hf_sdrop(=%s) specified in params.py will be used for all SRF files." % (
                params.hf.hf_kappa, params.hf.hf_sdrop)
            print"       If you need to specific hf_kappa and hf_sdrop value for each SRF, add hf_kappa_list and hf_sdrop_list to params_base.py"

    else:
        print "hf_kappa_list: ", hf_kappa_list
        print "hf_sdrop_list: ", hf_sdrop_list
        print "srf_files:", params.srf_file
        if len(hf_kappa_list) != len(hf_sdrop_list) or len(hf_kappa_list) != len(params.srf_files):
            print "Error: hf_kappa_list (len=%d), hf_sdrop_list (len=%d) and srf_files (len=%d) should be of the same length." % (
                len(hf_kappa_list), len(hf_sdrop_list), len(srf_files))
            sys.exit()

            # #TODO:add_name_suffix return the exact same name, seems to be legacy and doing nothing here
            # hf_run_name = add_name_suffix(hf_run_name,yes)
            # #append the hf_run_name to a list for later purpose
            # hf_run_names_list.append(hf_run_name)

            # hf_sim_basedir, bb_sim_basedir = os.path.join(params.sim_dir, 'HF', hf_run_name), os.path.join(params.sim_dir, 'BB', hf_run_name)
            # hf_sim_dir, bb_sim_dir = action_for_uncertainties(hf_sim_basedir,bb_sim_basedir, srf, slip, kappa, sdrop)

    utils.dump_yaml(root_dict, os.path.join(params.sim_dir, 'root_params.yaml'))


if __name__ == "__main__":
    main()
