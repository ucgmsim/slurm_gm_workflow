#!/usr/bin/env python2

import os.path
import sys
sys.path.append(os.path.abspath(os.path.curdir))
#from params_base import *
import argparse

from qcore import utils
params = utils.load_params('fault_params.yaml')

from collections import OrderedDict
old_params = True

#from params import hf_sdrop_list, hf_kappa_list, hf_sim_bin, hf_rvfac
# try:
#     from params import hf_sdrop_list, hf_kappa_list
# except ImportError:
#     print "Info: Old version of params.py supporting singular kappa and sdrop"
#     from params import hf_sdrop, hf_kappa
#     old_params = True
# finally:
#     from params import hf_sim_bin, hf_rvfac

import glob
# TODO: make sure that qcore is in the PYTHONPATH
from shared_workflow.shared import *

params_bb_uncertain = 'params_bb_uncertain.py'
params_uncertain='params_uncertain.py'


def q0():
    show_horizontal_line()
    print "Do you want site-specific computation? (To use a universal 1D profile, Select 'No')"
    show_horizontal_line()
    return show_yes_no_question()


def q1_generic(v_mod_1d_dir): 
    show_horizontal_line()
    print "Select one of 1D Velocity models (from %s)" %v_mod_1d_dir
    show_horizontal_line()
 
    v_mod_1d_options = glob.glob(os.path.join(v_mod_1d_dir,'*.1d'))
    v_mod_1d_options.sort()

    v_mod_1d_selected = show_multiple_choice(v_mod_1d_options)
    print v_mod_1d_selected #full path
    v_mod_1d_name = os.path.basename(v_mod_1d_selected).replace('.1d','')
#    print v_mod_1d_name

    return v_mod_1d_name,v_mod_1d_selected


def q1_site_specific(stat_file_path, hf_stat_vs_ref=None):
    show_horizontal_line()
    print "Auto-detecting site-specific info"
    show_horizontal_line()
    print "- Station file path: %s" %stat_file_path
    
    v_mod_1d_path = os.path.join(os.path.dirname(params.stat_file), "1D")
    if os.path.exists(v_mod_1d_path):
        print "- 1D profiles found at %s" %v_mod_1d_path
    else:
        print "Error: No such path exists: %s" %v_mod_1d_path
        sys.exit()
    if hf_stat_vs_ref == None:
        hf_stat_vs_ref_options = glob.glob(os.path.join(stat_file_path,'*.hfvs30ref'))
        if len(hf_stat_vs_ref_options) == 0:
            print "Error: No HF Vsref file was found at %s" % stat_file_path
            sys.exit()
        hf_stat_vs_ref_options.sort()

        show_horizontal_line()
        print "Select one of HF Vsref files"
        show_horizontal_line()
        hf_stat_vs_ref_selected=show_multiple_choice(hf_stat_vs_ref_options)
        print " - HF Vsref tp be used: %s" %hf_stat_vs_ref_selected
    else:
        hf_stat_vs_ref_selected = hf_stat_vs_ref
    return v_mod_1d_path, hf_stat_vs_ref_selected


def q2(v_mod_1d_name,srf,kappa,sdrop):
    hfVString='hf'+os.path.basename(params.hf.hf_sim_bin).split('_')[-1]
    hf_run_name=v_mod_1d_name+'_'+hfVString+'_rvf'+str(params.hf.hf_rvfac)+'_sd'+str(sdrop)+'_k'+str(kappa)
    hf_run_name=hf_run_name.replace('.','p')
    show_horizontal_line()
    print "- Vel. Model 1D: %s" %v_mod_1d_name
    print "- hf_sim_bin: %s" %os.path.basename(params.hf.hf_sim_bin)
    print "- hf_rvfac: %s" %params.hf.hf_rvfac
    print "- hf_sdrop: %s" %sdrop
    print "- hf_kappa: %s" %kappa
    print "- srf file: %s" %srf
#    yes = confirm_name(hf_run_name)
    yes=True
    return yes, hf_run_name


def store_params(params_base_bb_dict):
    f=open(os.path.join(params.sim_dir,"params_base_bb.py"),"w")
    keys = params_base_bb_dict.keys()
    for k in keys:
        val = params_base_bb_dict[k]
        if type(val) == str:
            val="'%s'"%val
        f.write("%s=%s\n"%(k,val))
    f.close()
    #neither of these vars are modified or declared here, no point of returning it.
#    return hf_dir, bb_dir


def action_for_uncertainties(hf_sim_basedir,bb_sim_basedir,srf,slip,kappa,sdrop):
    dirs = []
    srf_basename = os.path.splitext(os.path.basename(srf))[0] #take the filename only
    hf_sim_dir = os.path.join(hf_sim_basedir,srf_basename)
    bb_sim_dir = os.path.join(bb_sim_basedir,srf_basename)
    dirs.append(hf_sim_dir)
    dirs.append(bb_sim_dir)
    params_uncertain_path=os.path.join(params.lf_sim_root_dir,srf_basename,params_uncertain)
    print params_uncertain_path
    execfile(params_uncertain_path,globals())

    verify_user_dirs(dirs)
    params_bb_uncertain_file = os.path.join(bb_sim_dir,params_bb_uncertain) 
    with open(params_bb_uncertain_file,'w') as f:
        f.write("hf_accdir='%s'\n"%os.path.join(hf_sim_dir,"Acc"))
        f.write("hf_veldir='%s'\n"%os.path.join(hf_sim_dir,"Vel"))
        f.write("bb_accdir='%s'\n"%os.path.join(bb_sim_dir,"Acc"))
        f.write("bb_veldir='%s'\n"%os.path.join(bb_sim_dir,"Vel"))
        f.write("hf_resume=True\n")
        f.write("bb_resume=True\n")
        f.write("hf_slip='%s'\n"%slip)
        f.write("vel_dir='%s'\n"%params.lf.vel_dir)
        f.write("hf_kappa='%s'\n"%kappa)
        f.write("hf_sdrop='%s'\n"%sdrop)
    
    params_hf_uncertain_file = os.path.join(hf_sim_dir,params_bb_uncertain)
    print params_hf_uncertain_file 
    try:
        os.symlink(params_bb_uncertain_file, params_hf_uncertain_file)
    except OSError as e:
        print e
    return hf_sim_dir, bb_sim_dir    

#    try: 
#        set_permission(hf_sim_basedir)
#    except OSError as e:
#        print e
#    try:
#        set_permission(bb_sim_basedir)
#    except OSError as e:
#        print e



    #creating symbolic link between matching HF and BB directories
#    try: 
#        os.symlink(bb_sim_dir,os.path.join(hf_sim_dir,"BB"))
#    except OSError:
#        print "Directory already exists: %s" %os.path.join(hf_sim_dir,"BB")
#    try: 
#        os.symlink(hf_sim_dir,os.path.join(bb_sim_dir,"HF"))
#    except OSError:
#        print "Directory already exists: %s" %os.path.join(bb_sim_dir,"HF")



def main():
    global hf_kappa_list #no idea why hf_kappa_list is imported, but not usable in this function without this.
    global hf_sdrop_list

    parser = argparse.ArgumentParser()
    parser.add_argument('--v1d', default=None, type=str, help="the full path pointing to the generic v1d file")
    parser.add_argument('--site_v1d_dir',default=None, type=str, help="the to the directory containing site specific files, hf_stat_vs_ref must be provied as well if this is provided")
    parser.add_argument('--hf_stat_vs_ref',default=None, type=str,help="site_v1d_dir must be provied as well if this is provided")
    args = parser.parse_args()

    show_horizontal_line(c="*")
    print " "*37+"EMOD3D HF/BB Preparationi Ver."+ params.bin_process_ver
    show_horizontal_line(c="*")

    params_base_bb_dict = OrderedDict()

#    params_base_bb_dict['rand_reset']=False #by default. But it may give less deterministic
    if args.v1d != None :
        v_mod_1d_selected = args.v1d
        v_mod_1d_name = os.path.basename(v_mod_1d_selected).replace('.1d','')
        params_base_bb_dict['site_specific']=False
        params_base_bb_dict['hf_v_model']=v_mod_1d_selected
    #TODO:add in logic for site specific as well, if the user provided as args
    elif args.site_v1d_dir != None and args.hf_stat_vs_ref != None:
        v_mod_1d_path,hf_stat_vs_ref=q1_site_specific(args.site_v1d_dir, hf_stat_vs_ref=args.hf_stat_vs_ref)
        v_mod_1d_name = "Site_Specific"
        params_base_bb_dict['site_specific']=True
        params_base_bb_dict['hf_v_model_path']=v_mod_1d_path
        params_base_bb_dict['hf_stat_vs_ref']=hf_stat_vs_ref
        params_base_bb_dict['rand_reset']=True
    else:    
        is_site_specific_id = q0()
        if is_site_specific_id:
            v_mod_1d_path,hf_stat_vs_ref=q1_site_specific(os.path.dirname(params.stat_file))
            v_mod_1d_name = "Site_Specific"
            params_base_bb_dict['site_specific']=True
            params_base_bb_dict['hf_v_model_path']=v_mod_1d_path
            params_base_bb_dict['hf_stat_vs_ref']=hf_stat_vs_ref
            params_base_bb_dict['rand_reset']=True  

        else:
            v_mod_1d_name, v_mod_1d_selected = q1_generic(params.v_mod_1d_dir)
            params_base_bb_dict['site_specific']=False
            params_base_bb_dict['hf_v_model']=v_mod_1d_selected

    params_base_bb_dict['v_mod_1d_name']=v_mod_1d_name


    #globals_dict = globals()

    if old_params:
        hf_kappa_list = [params.hf.hf_kappa]*len(params.srf_file)
        hf_sdrop_list = [params.hf.hf_sdrop]*len(params.srf_file)
        if len(params.srf_file) > 1:
            print "Info: You have specified multiple SRF files."
            print "      A single hf_kappa(=%s) and hf_sdrop(=%s) specified in params.py will be used for all SRF files." %(params.hf.hf_kappa, params.hf.hf_sdrop)
            print"       If you need to specific hf_kappa and hf_sdrop value for each SRF, add hf_kappa_list and hf_sdrop_list to params_base.py"

    else:
        print "hf_kappa_list: ", hf_kappa_list
        print "hf_sdrop_list: ", hf_sdrop_list
        print "srf_files:", params.srf_file
        if len(hf_kappa_list) != len(hf_sdrop_list) or len(hf_kappa_list) != len(params.srf_files):
            print "Error: hf_kappa_list (len=%d), hf_sdrop_list (len=%d) and srf_files (len=%d) should be of the same length."%(len(hf_kappa_list),len(hf_sdrop_list), len(srf_files))
            sys.exit()


    hf_run_names_list=[]
    for i in range(len(hf_kappa_list)):
        kappa = hf_kappa_list[i]
        sdrop = hf_sdrop_list[i]
        srf = params.srf_file[i]
        slip = params.hf.hf_slip[i]
        yes, hf_run_name = q2(v_mod_1d_name,srf, kappa,sdrop)
        #TODO:add_name_suffix return the exact same name, seems to be legacy and doing nothing here 
        hf_run_name = add_name_suffix(hf_run_name,yes)
        #append the hf_run_name to a list for later purpose
        hf_run_names_list.append(hf_run_name)

        hf_sim_basedir, bb_sim_basedir = os.path.join(params.hf_dir,hf_run_name), os.path.join(params.bb_dir, hf_run_name)
        hf_sim_dir, bb_sim_dir = action_for_uncertainties(hf_sim_basedir,bb_sim_basedir, srf, slip, kappa, sdrop)

        params_base_bb_dict['hf_run_name'] = hf_run_name
        params_base_bb_dict['hf_acc_dir'] = os.path.join(hf_sim_dir,"Acc")
        params_base_bb_dict['hf_veldir'] = os.path.join(hf_sim_dir,"Vel")
        params_base_bb_dict['bb_acc_dir'] = os.path.join(bb_sim_dir,"Acc")
        params_base_bb_dict['bb_veldir'] = os.path.join(bb_sim_dir,"Vel")
        params_base_bb_dict['hf_resume'] = True
        params_base_bb_dict['bb_resume'] = True
        utils.dump_yaml(params_base_bb_dict, os.path.join(hf_sim_dir, 'params_bb_uncertain.yaml'))
    params_base_bb_dict.pop('hf_run_name', None)
    params_base_bb_dict['hf_run_names'] = hf_run_names_list
    #store the parameters in params_base_bb.py 
    store_params(params_base_bb_dict)

    params.bb.update(params_base_bb_dict)
    print("ssss",type(params))
    utils.dump_yaml(params, 'fault_params_b.yaml', obj_type=utils.DotDictify)

    

if __name__ == "__main__":
    main() 

