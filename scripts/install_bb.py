#!/usr/bin/env python
"""Script for creating the bb folder structure and params"""
import glob
import argparse

from qcore import utils

from shared_workflow import load_config
from shared_workflow.shared import *


sys.path.append(os.path.abspath(os.path.curdir))

workflow_config = load_config.load(os.path.dirname(os.path.realpath(__file__)), "workflow_config.json")
global_root = workflow_config["global_root"]
tools_dir = os.path.join(global_root, 'opt/maui/emod3d/3.0.4-gcc/bin')
emod3d_version = workflow_config["emod3d_version"]
V_MOD_1D_DIR = os.path.join(global_root, 'VelocityModel', 'Mod-1D')
params = utils.load_sim_params('sim_params.yaml')
root_dict = utils.load_yaml(params.root_yaml_path)

def q0():
    show_horizontal_line()
    print("Do you want site-specific computation? "
          "(To use a universal 1D profile, Select 'No')")
    show_horizontal_line()
    return show_yes_no_question()


def q1_generic(v_mod_1d_dir):
    show_horizontal_line()
    print("Select one of 1D Velocity models (from %s)" % v_mod_1d_dir)
    show_horizontal_line()

    v_mod_1d_options = glob.glob(os.path.join(v_mod_1d_dir, '*.1d'))
    v_mod_1d_options.sort()

    v_mod_1d_selected = show_multiple_choice(v_mod_1d_options)
    print(v_mod_1d_selected)  # full path
    v_mod_1d_name = os.path.basename(v_mod_1d_selected).replace('.1d', '')

    return v_mod_1d_name, v_mod_1d_selected


def q1_site_specific(stat_file_path, hf_stat_vs_ref=None, v1d_mod_dir=None):
    show_horizontal_line()
    print("Auto-detecting site-specific info")
    show_horizontal_line()
    print("- Station file path: %s" % stat_file_path)

    if v1d_mod_dir is not None:
        v_mod_1d_path = v1d_mod_dir
    else:
        v_mod_1d_path = os.path.join(os.path.dirname(stat_file_path), "1D")
    if os.path.exists(v_mod_1d_path):
        print("- 1D profiles found at %s" % v_mod_1d_path)
    else:
        print("Error: No such path exists: %s" % v_mod_1d_path)
        sys.exit()
    if hf_stat_vs_ref is None:
        hf_stat_vs_ref_options = glob.glob(
            os.path.join(stat_file_path, '*.hfvs30ref'))
        if len(hf_stat_vs_ref_options) == 0:
            print("Error: No HF Vsref file was found at %s" % stat_file_path)
            sys.exit()
        hf_stat_vs_ref_options.sort()

        show_horizontal_line()
        print("Select one of HF Vsref files")
        show_horizontal_line()
        hf_stat_vs_ref_selected = show_multiple_choice(hf_stat_vs_ref_options)
        print(" - HF Vsref tp be used: %s" % hf_stat_vs_ref_selected)
    else:
        hf_stat_vs_ref_selected = hf_stat_vs_ref
    return v_mod_1d_path, hf_stat_vs_ref_selected


def q2(v_mod_1d_name, srf, kappa, sdrop):
    hf_sim_bin = os.path.join(tools_dir, 'hb_high_v5.4.5_np2mm+')
    hfVString = 'hf' + os.path.basename(hf_sim_bin).split('_')[-1]
    hf_run_name = "{}_{}_rvf{}_sd{}_k{}".format(
        v_mod_1d_name, hfVString, str(params.hf.hf_rvfac),
        str(sdrop), str(kappa))
    hf_run_name = hf_run_name.replace('.', 'p')
    show_horizontal_line()
    print("- Vel. Model 1D: %s" % v_mod_1d_name)
    print("- hf_sim_bin: %s" % os.path.basename(hf_sim_bin))
    print("- hf_rvfac: %s" % params.hf.hf_rvfac)
    print("- hf_sdrop: %s" % sdrop)
    print("- hf_kappa: %s" % kappa)
    print("- srf file: %s" % srf)
    #    yes = confirm_name(hf_run_name)
    yes = True
    return yes, hf_run_name


def store_params(root_dict):
    f = open(os.path.join(params.sim_dir, "params_base_bb.py"), "w")
    keys = list(root_dict.keys())
    for k in keys:
        val = root_dict[k]
        if type(val) == str:
            val = "'%s'" % val
        f.write("%s=%s\n" % (k, val))
    f.close()


def install_bb(v1d, site_v1d_dir, hf_stat_vs_ref):
    show_horizontal_line(c="*")
    print(" " * 37 + "EMOD3D HF/BB Preparationi Ver.slurm")
    show_horizontal_line(c="*")

    root_dict['bb'] = {}

    if v1d is not None:
        v_mod_1d_selected = v1d
        root_dict['bb']['site_specific'] = False
        root_dict['v_mod_1d_name'] = v_mod_1d_selected
    # TODO:add in logic for site specific as well, if the user provided as args
    elif site_v1d_dir is not None and hf_stat_vs_ref is not None:
        v_mod_1d_path, hf_stat_vs_ref = q1_site_specific(
            os.path.dirname(params.stat_file),
            hf_stat_vs_ref=hf_stat_vs_ref, v1d_mod_dir=site_v1d_dir)

        root_dict['bb']['site_specific'] = True
        root_dict['v_mod_1d_name'] = v_mod_1d_path
        root_dict['hf_stat_vs_ref'] = hf_stat_vs_ref
        root_dict['bb']['rand_reset'] = True
    else:
        is_site_specific_id = q0()
        if is_site_specific_id:
            v_mod_1d_path, hf_stat_vs_ref = q1_site_specific(
                os.path.dirname(params.stat_file))
            root_dict['bb']['site_specific'] = True
            root_dict['v_mod_1d_name'] = v_mod_1d_path
            root_dict['hf_stat_vs_ref'] = hf_stat_vs_ref
            root_dict['bb']['rand_reset'] = True
        else:
            v_mod_1d_name, v_mod_1d_selected = q1_generic(V_MOD_1D_DIR)
            root_dict['bb']['site_specific'] = False
            root_dict['v_mod_1d_name'] = v_mod_1d_selected

    utils.dump_yaml(root_dict, params.root_yaml_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--v1d', default=None, type=str,
                        help="the full path pointing to the generic v1d file")
    parser.add_argument('--site_v1d_dir', default=None, type=str,
                        help="the to the directory containing site specific "
                             "files, hf_stat_vs_ref must be provied as well if "
                             "this is provided")
    parser.add_argument('--hf_stat_vs_ref', default=None, type=str,
                        help="site_v1d_dir must be provied as well "
                             "if this is provided")
    args = parser.parse_args()

    install_bb(args.v1d, args.site_v1d_dir, args.hf_stat_vs_ref)


if __name__ == "__main__":
    main()
