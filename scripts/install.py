#!/usr/bin/env python2
"""python ~/slurm_gm_workflow/scripts/install.py --user_root ~/Albury_newman --srf_dir ~/Albury_newman/Data/Sources/ --vm_dir ~/Albury/Data/VMs/ --version gmsim_v18.5.3"""

import os
import glob
import shutil
import getpass
import fnmatch

import datetime
# from shared_workflow.load_config import load
from shared_workflow import load_config as ldcfg
import ConfigParser
import argparse
from management import create_mgmt_db

# TODO: namespacing
from shared_workflow.shared import *

from qcore import utils

print
workflow_config = ldcfg.load(os.path.dirname(os.path.realpath(__file__)), "workflow_config.json")
workflow_root = workflow_config['gm_sim_workflow_root']
global_root = workflow_config["global_root"]
tools_dir = os.path.join(global_root, 'EMOD3D/tools')
bin_process_dir = os.path.join(global_root, 'workflow/scripts')
emod3d_version = workflow_config["emod3d_version"]
params_vel = workflow_config['params_vel']

run_dir = workflow_config['runfolder_path']
user = getpass.getuser()
user_root = os.path.join(run_dir, user)  # global_root
srf_default_dir = os.path.join(global_root, 'RupModel')
vel_mod_dir = os.path.join(global_root, 'VelocityModels')
# recipe_dir = os.path.join(global_root, "workflow/templates")
recipe_dir = workflow_config['templates_dir']
v_mod_1d_dir = os.path.join(global_root, 'VelocityModel', 'Mod-1D')
gmsa_dir = os.path.join(global_root, 'groundMotionStationAnalysis')
stat_dir = os.path.join(global_root, 'StationInfo')

latest_ll_dir = os.path.join(global_root, 'StationInfo/grid')
latest_ll = 'non_uniform_with_real_stations_latest'

# default values
# TODO: after enabling different dt for LF and HF, this might need to change
default_dt = 0.005
default_hf_dt = 0.005


def q_accept_custom_rupmodel():
    show_horizontal_line()
    print "Do you wish to use custom rupture files?"
    show_horizontal_line()
    return show_yes_no_question()


def q_custom_rupmodel_path():
    verified = False
    while not verified:
        rupture_path = raw_input("Enter path to custom Rupture Model (the parent of Srf/Stoch SRF directory): ")
        if not os.path.exists(rupture_path):
            print "Specified path doesn't exist. Try again"
            continue
        verified = True
    return rupture_path


def q_select_rupmodel_dir(mysrf_dir):
    show_horizontal_line()
    print "Select Rupture Model - Step 1."
    show_horizontal_line()
    srf_options = os.listdir(mysrf_dir)
    srf_options = [x for x in srf_options if
                   os.path.isdir(os.path.join(mysrf_dir, x)) and not x.startswith('.')]  # avoid displaying .git
    srf_options.sort()
    if len(srf_options) == 0:  # this is a dead end
        print "No valid rupture model found"
        sys.exit()

    srf_selected = show_multiple_choice(srf_options)
    print srf_selected
    srf_selected_dir = os.path.join(mysrf_dir, srf_selected, "Srf")
    try:
        srf_file_options = os.listdir(srf_selected_dir)
    except OSError:
        print "!! Srf directory not found. Going into %s" % srf_selected
        return q_select_rupmodel_dir(os.path.join(mysrf_dir, srf_selected))

    return srf_selected, srf_selected_dir, srf_file_options


def q_select_rupmodel(srf_selected_dir, srf_file_options):
    show_horizontal_line()
    print "Select Rupture Model - Step 2."
    show_horizontal_line()
    srf_file_options.sort()
    # choose one srf (instead of multiple to fit the new versioning
    # singular returns a str instead of str[]. reformat
    srf_files_selected = []
    srf_files_selected.append(show_multiple_choice(srf_file_options, singular=True))  # always in list

    print srf_files_selected

    for srf_file_selected in srf_files_selected:
        srf_file_path = os.path.join(srf_selected_dir, srf_file_selected)
        stoch_selected_dir = os.path.abspath(os.path.join(srf_selected_dir, os.path.pardir, 'Stoch'))
        if not os.path.isdir(stoch_selected_dir):
            print "Error: Corresponding Stoch directory is not found:\n%s" % stoch_selected_dir
            sys.exit()
        stoch_file_path = os.path.join(stoch_selected_dir, srf_file_selected).replace(".srf", ".stoch")
        if not os.path.isfile(stoch_file_path):
            print "Error: Corresponding Stock file is not found:\n%s" % stoch_file_path
            sys.exit()

        print "Corresponding Stock file is also found:\n%s" % stoch_file_path

    return srf_files_selected, srf_file_path, stoch_file_path


def q_select_vel_model(vel_mod_dir):
    show_horizontal_line()
    print "Select one of available VelocityModels (from %s)" % vel_mod_dir
    show_horizontal_line()

    v_mod_ver_options = []
    for root, dirnames, filenames in os.walk(vel_mod_dir):
        # returns the folder that contains params_vel.py
        for filename in fnmatch.filter(filenames, params_vel):
            v_mod_ver_options.append(root)

    v_mod_ver_options.sort()
    v_mod_ver = show_multiple_choice(v_mod_ver_options)
    print v_mod_ver
    vel_mod_dir = os.path.join(vel_mod_dir, v_mod_ver)
    print vel_mod_dir, params_vel
    params_vel_path = os.path.join(vel_mod_dir, params_vel)
    if not os.path.exists(params_vel_path):
        print "Error: %s doesn't exist" % params_vel_path
        sys.exit()

    return v_mod_ver, vel_mod_dir, params_vel_path


def q_real_stations():
    show_horizontal_line()
    print "Do you wish to use real stations?"
    show_horizontal_line()
    return show_yes_no_question()


def q_select_stat_file(remove_fd=False):
    show_horizontal_line()
    print "Select one of available Station list (from %s)" % stat_dir
    show_horizontal_line()
    stat_files = glob.glob(os.path.join(stat_dir, '*.ll'))
    stat_files = [os.path.basename(x) for x in stat_files]
    if remove_fd:
        stat_files = [x for x in stat_files if not x.startswith('fd_')]

    stat_files.sort()
    stat_file = show_multiple_choice(stat_files)
    print stat_file
    stat_file_path = os.path.join(stat_dir, stat_file)
    print stat_file_path
    return stat_file_path


def q_select_vs30_file():
    show_horizontal_line()
    print "Select one of available vs30 (from %s)" % stat_dir
    show_horizontal_line()
    vs30_files = glob.glob(os.path.join(stat_dir, '*.vs30'))
    vs30_files = [os.path.basename(x) for x in vs30_files]

    vs30_files.sort()
    vs30_file = show_multiple_choice(vs30_files)
    print vs30_file
    vs30_file_path = os.path.join(stat_dir, vs30_file)
    print vs30_file_path
    vs30ref_file_path = vs30_file_path.replace(".vs30", ".vs30ref")
    if not os.path.isfile(vs30ref_file_path):
        print "Error: Corresponding vs30ref file is not found:\n%s" % vs30ref_file_path
        sys.exit()

    return vs30_file_path, vs30ref_file_path


def q_get_run_name(HH, srf_selected, v_mod_ver, emod3d_version):
    # automatic generation of the run name (LP here only, HF and BB come later after declaration of HF and BB parameters).
    userString = datetime.date.today().strftime("%y%m%d")  # additional string to customize (today's date for starters)
    hString = '-h' + HH
    srfString = srf_selected  # use full name of RupModel directory #srf_selected.split("_")[0]
    vModelString = 'VM' + str(v_mod_ver)
    vString = '_EMODv' + emod3d_version
    run_name = (srfString + '_' + vModelString + hString + vString + '_' + userString).replace('.', 'p').replace('/',
                                                                                                                 '_')  # replace the decimal points with p
    # LPSim-2010Sept4_bev01_VMv1p64-h0p100_EMODv3p0p4_19May2016

    yes = confirm_name(run_name)
    return yes, run_name


def q_statcords_convert():
    show_horizontal_line()
    print "Do you want the statcords and fd_statlist to be automatically generated? (Recommended)"
    show_horizontal_line()
    return show_yes_no_question()


def q_model_params():
    show_horizontal_line()
    print "Do you want the model params to be automatically generated? (Recommended)"
    show_horizontal_line()
    return show_yes_no_question()


def q_final_confirm(run_name, yes_statcords, yes_model_params):
    print ""
    show_horizontal_line(c="*")
    print "S U M M A R Y"
    show_horizontal_line(c="*")
    print ""
    print "- Simulation directory: %s" % run_name
    #    print "- Recipe to be copied from \n%s" %recipe_selected_dir
    print "- Statcoords generated: %s" % yes_statcords
    # print "- Model Params generated: %s\n"%yes_model_params
    show_horizontal_line(c="*")
    print "Do you wish to proceed?"
    return show_yes_no_question()


def create_params_dict(version, sim_dir, event_name, run_name, run_dir, vel_mod_dir, srf_dir, srf_file, stoch_file,
                       params_vel_path, stat_file_path, vs30_file_path, vs30ref_file_path, MODEL_LAT, MODEL_LON,
                       MODEL_ROT, hh, nx, ny, nz, sufx, sim_duration, flo, vel_mod_params_dir, yes_statcords,
                       yes_model_params, dt=default_dt, hf_dt=default_hf_dt):
    lf_sim_root_dir = os.path.join(sim_dir, "LF")
    hf_dir = os.path.join(sim_dir, "HF")
    bb_dir = os.path.join(sim_dir, 'BB')
    dir_list = [sim_dir, lf_sim_root_dir, hf_dir, bb_dir]

    if not os.path.isdir(user_root):
        dir_list.insert(0, user_root)

    verify_user_dirs(dir_list)

    for filename in glob.glob(os.path.join(recipe_dir, '*.*')):
        if filename == "README.md":
            continue
        shutil.copy(filename, sim_dir)

        # TODO: the next two lines are two files for old post-processing
        # shutil.copy(os.path.join(gmsa_dir,"parametersStation.py"),sim_dir)
        # shutil.copy(os.path.join(gmsa_dir,"runPostProcessStation.ll"),sim_dir)
    #    exe('ln -s %s/submit_emod3d.py %s'%(bin_process_dir,sim_dir))
    shutil.copy(os.path.join(workflow_root, "version"), sim_dir)
    shutil.copy(os.path.join(bin_process_dir, "submit.sh"), sim_dir)

    if not yes_model_params:
        print "Generation of model params has been skipped."
        print "Re-directing related params to files under %s" % vel_mod_dir
        vel_mod_params_dir = vel_mod_dir

    root_params_dict = {}
    fault_params_dict = {}
    sim_params_dict = {}
    vm_params_dict = {}

    sim_params_dict['run_name'] = run_name

    # select during install
    root_params_dict['version'] = version

    root_params_dict['dt'] = 0.02
    root_params_dict['stat_file'] = stat_file_path

    # potential remove
    sim_params_dict['user_root'] = user_root
    sim_params_dict['run_dir'] = run_dir

    sim_params_dict['sim_dir'] = sim_dir

    sim_params_dict['srf_file'] = srf_file
    fault_params_dict['vel_mod_dir'] = vel_mod_dir
    sim_params_dict['params_vel'] = params_vel_path
    sim_params_dict['sim_duration'] = sim_duration
    root_params_dict['flo'] = flo

    vm_params_dict['MODEL_LAT'] = MODEL_LAT
    vm_params_dict['MODEL_LON'] = MODEL_LON
    vm_params_dict['MODEL_ROT'] = MODEL_ROT
    vm_params_dict['hh'] = hh
    vm_params_dict['nx'] = nx
    vm_params_dict['nz'] = nz
    vm_params_dict['ny'] = ny

    vm_params_dict['sufx'] = sufx
    vm_params_dict['GRIDFILE'] = os.path.join(vel_mod_params_dir, 'gridfile%s' % sufx)
    vm_params_dict['GRIDOUT'] = os.path.join(vel_mod_params_dir, 'gridout%s' % sufx)
    vm_params_dict['MODEL_COORDS'] = os.path.join(vel_mod_params_dir, 'model_coords%s' % sufx)
    vm_params_dict['MODEL_PARAMS'] = os.path.join(vel_mod_params_dir, 'model_params%s' % sufx)
    vm_params_dict['MODEL_BOUNDS'] = os.path.join(vel_mod_params_dir, 'model_bounds%s' % sufx)

    root_params_dict['hf'] = {}
    sim_params_dict['hf'] = {}
    root_params_dict['hf']['hf_dt'] = hf_dt
    root_params_dict['hf']['hf_sdrop'] = 50
    root_params_dict['hf']['hf_kappa'] = 0.045
    root_params_dict['hf']['hf_rvfac'] = 0.8
    root_params_dict['hf']['hf_path_dur'] = 1
    sim_params_dict['hf']['hf_slip'] = stoch_file

    sim_params_dict['bb'] = {}

    # potential remove
    sim_params_dict['emod3d'] = {}
    sim_params_dict['emod3d']['n_proc'] = 512
    if stat_file_path == "":
        # stat_path seems to empty, assigning all related value to latest_ll
        print "stat_file_path is not specified."
        print "Using %s" % latest_ll
        run_stat_dir = os.path.join(stat_dir, event_name)
        stat_file_path = os.path.join(run_stat_dir, event_name + '.ll')
        vs30_file_path = os.path.join(run_stat_dir, event_name + '.vs30')
        vs30ref_file_path = os.path.join(run_stat_dir, event_name + '.vs30ref')
        # creating sub-folder for run_name
        # check if folder already exist
        if not os.path.isdir(run_stat_dir):
            # folder not exist, creating
            os.mkdir(run_stat_dir)
            # making symbolic link to latest_ll
            cmd = "ln -s %s %s" % (os.path.join(latest_ll_dir, latest_ll + '.ll'), stat_file_path)
            exe(cmd)
            # making symbolic link to lastest_ll.vs30 and .vs30ref
            cmd = "ln -s %s %s" % (os.path.join(latest_ll_dir, latest_ll + '.vs30'), vs30_file_path)
            exe(cmd)
            cmd = "ln -s %s %s" % (os.path.join(latest_ll_dir, latest_ll + '.vs30ref'), vs30ref_file_path)
            exe(cmd)
    root_params_dict['stat_vs_est'] = vs30_file_path
    root_params_dict['stat_vs_ref'] = vs30ref_file_path

    if stat_file_path is not None:
        sim_params_dict['stat_file'] = stat_file_path

    # if yes_model_params:
    #     print "Producing model params. It may take a minute or two"
    #     from gen_coords import gen_coords
    #     gen_coords()
    #     print "Done"

    if yes_statcords:
        print "Producing statcords and FD_STATLIST. It may take a minute or two"

        # Create Stat_cord & statList
        import statlist2gp
        fd_statcords, fd_statlist = statlist2gp.main(sim_params_dict, vm_params_dict, stat_file=stat_file_path)
        print "Done"
        sim_params_dict['stat_coords'] = fd_statcords
        sim_params_dict['FD_STATLIST'] = fd_statlist
    else:
        print "Generation of statcords is skipped. You need to fix params_base.py manually"
    return root_params_dict, fault_params_dict, sim_params_dict, vm_params_dict


def show_instruction(sim_dir):
    try:
        print "Removing probably incomplete " + os.path.join(sim_dir, "params_base.pyc")
        os.remove(os.path.join(sim_dir, "params_base.pyc"))
    except Exception, e:
        print e.args
        print "Could not remove params_base.pyc"

    show_horizontal_line()
    print "Instructions"
    show_horizontal_line()
    print "   For Simulation (Slurm based system)"
    print "    1.   cd %s" % sim_dir
    print "    2.   Edit params.py and run_emod3d.sl.template as needed"
    print "    3.   python $gmsim/workflow/scripts/submit_emod3d.py"
    print "    4.   python $gmsim/workflow/scripts/submit_post_emod3d.py"
    print "    5.   python $gmsim/workflow/scripts/install_bb.py"
    print "    6.   python $gmsim/workflow/scripts/submit_hf.py and python $gmsim/workflow/scripts/submit_bb.py"
    print "    Note: If you did not submit one or more .sl scripts, just do 'sbatch slurm_script.sl"
    print "   For Plotting (hypocentre)"
    print "    1.   Plotting station based data (IMs, Vs30, Obs PGA, pSA): plot_stations.py {datafile}.ll"
    print "    2.   Plotting timeslice-based data (PGV,MMI,animation): plot_transfer.py auto %s" % sim_dir
    print "    3.   PGV,MMI: plot_ts_sum.py Timeslice sequence: plot_ts.py"


def q_wallclock():
    show_horizontal_line()
    print "Do you wish to modify the wallclock time limit?"
    show_horizontal_line()
    return show_yes_no_question()


def wc_usage():
    print r'input value error. input does not match formate : %H:%M:%S'
    print r'Must not exceed the limit where: %H <= 23, %M <= 59, %S <= 59'


def get_input_wc():
    show_horizontal_line()
    try:
        user_input_wc = datetime.datetime.strptime(
            str(raw_input("Enter the WallClock time limit you will like to use: ")), "%H:%M:%S").time()
    except ValueError:
        wc_usage()
        user_input_wc = get_input_wc()
    show_horizontal_line()

    return user_input_wc


def wallclock(sim_dir):
    show_horizontal_line()
    print "Fetching WallClock time estimation from DB"
    show_horizontal_line()
    try:
        import params
    except ImportError:
        print "import params failed. check sys.path"
    # print "--testing params var:",params.nx
    try:
        import wct
    except ImportError:
        print "cannnot import wct.py. please check sys.path"
    else:
        # retrive data from DB. same lines as in wct.py
        # TODO: fix this hardcoded value
        db = wct.WallClockDB("wallclock.sqlite")
        print db.est
        nx = int(params.nx)
        ny = int(params.ny)
        nz = int(params.nz)
        sim_duration = int(float(params.sim_duration))
        num_procs = int(params.n_proc)
        est = db.estimate_wall_clock_time(nx, ny, nz, sim_duration, num_procs)
        # data retrived, and presented(in db.estimate_wall_clock_time)

        # check if user wants to adjust wall clock limit
        # yes_change_wc = q_wallclock()
        yes_change_wc = True
        '''#old behavior that writes wall_clock_limit to params_base.py, which is no longer optimal.
        try:
            f = open(os.path.join(sim_dir,"params_base.py"),"a")
        except IOErroe:
            print "cannot open file :",emod3d_temp_dir
        else:
            if yes_change_wc:
                user_input_wc = get_input_wc()
                #writes value into params_base.py
                print "Changing the WallClock limit in parames_base.py  into : %s "%user_input_wc
                f.write("wall_clock_limit = '%s'\n" %user_input_wc)
            else:
                print "User decided not to change WallClock time in template.\n"
                #Gives wall_clock_limit a blank value so that in submit_*.py it knows not to change it.
                f.write("wall_clock_limit = ''\n")
            f.close()
        '''

        # new behavior: instead of writing to params_base.py, return a datetime value.
        if yes_change_wc:
            user_input_wc = get_input_wc()
            return user_input_wc


def main_local():
    show_horizontal_line(c="*")
    print " " * 37 + "EMOD3D Job Preparation Ver. Slurm"
    show_horizontal_line(c="*")

    srf_dir = srf_default_dir  # the above is perhaps unnecessary

    srf_selected, srf_selected_dir, srf_file_options = q_select_rupmodel_dir(srf_dir)
    srf_files_selected, srf_file, stoch_file = q_select_rupmodel(srf_selected_dir, srf_file_options)
    #    HH = q3() ## HH is taken directly from params_vel.py
    v_mod_ver, vel_mod_dir_full, params_vel_path = q_select_vel_model(vel_mod_dir)

    execfile(params_vel_path, globals())  # import params_vel, to retrieve HH

    yes_real_stations = q_real_stations()
    if yes_real_stations:
        stat_file_path = q_select_stat_file(remove_fd=True)
    else:
        stat_file_path = None

    vs30_file_path, vs30ref_file_path = q_select_vs30_file()

    yes, run_name = q_get_run_name(hh, srf_selected, v_mod_ver, emod3d_version)
    while True:
        run_name = add_name_suffix(run_name, yes)
        sim_dir = os.path.join(user_root, run_name)
        yes = True
        if not os.path.exists(sim_dir):  # sim_dir is new
            break
        else:
            print "Error: %s already exists. Add an additional suffix" % sim_dir
            yes = False

    # yes_statcords = q_statcords_convert()
    yes_statcords = True  # Always. If it becomes optional, params_base may have no entry of stat_coords and FD_STATLIST

    # yes_model_params = q_model_params()
    yes_model_params = False  # Always. To support statgrid, it is better to keep this way.

    final_yes = q_final_confirm(run_name, yes_statcords, yes_model_params)

    if not final_yes:
        print "Installation exited"
        sys.exit()

    vel_mod_params_dir = sim_dir  # we will keep model params in the same dir as sim_dir
    #    vel_mod_params_dir = os.path.join(global_root, "VelocityModel/SthIsland/ModelParams")

    event_name = ""
    root_params_dict, fault_params_dict, sim_params_dict, vm_params_dict = create_params_dict(args.version, sim_dir,
                                                                                              event_name, run_name,
                                                                                              run_dir, vel_mod_dir_full,
                                                                                              srf_dir, srf_file, stoch_file,
                                                                                              params_vel_path,
                                                                                              stat_file_path,
                                                                                              vs30_file_path,
                                                                                              vs30ref_file_path,
                                                                                              MODEL_LAT, MODEL_LON,
                                                                                              MODEL_ROT, hh, nx,
                                                                                              ny, nz, sufx,
                                                                                              sim_duration, flo,
                                                                                              vel_mod_params_dir,
                                                                                              yes_statcords,
                                                                                              yes_model_params)

    fault_dir = os.path.abspath(os.path.join(sim_dir, os.pardir))
    create_mgmt_db.create_mgmt_db([], fault_dir, srf_files=srf_file)

    root_params_dict['mgmt_db_location'] = fault_dir
    utils.dump_yaml(root_params_dict, os.path.join(sim_dir, 'root_params.yaml'))
    utils.dump_yaml(fault_params_dict, os.path.join(sim_dir, 'fault_params.yaml'))
    utils.dump_yaml(sim_params_dict, os.path.join(sim_dir, 'sim_params.yaml'))
    utils.dump_yaml(vm_params_dict, os.path.join(fault_params_dict['vel_mod_dir'], 'vm_params.yaml'))
    print "Installation completed"
    show_instruction(sim_dir)


def main_remote(cfg):
    config = ConfigParser.RawConfigParser()
    config.read(cfg)

    event_name = config.get('gmsim', 'event_name')
    run_name = config.get('gmsim', 'run_name')

    vel_mod_dir = config.get('remote', 'vel_mod_dir')
    srf_file_path = config.get('remote', 'srf_path')
    stat_file_path = config.get('remote', 'stat_path')

    stoch_file_path = srf_file_path.replace('Srf', 'Stoch').replace('srf', 'stoch')
    vs30_file_path = stat_file_path.replace('.ll', '.vs30')
    vs30ref_file_path = stat_file_path.replace('.ll', '.vs30ref')

    srf_file = filter(None, (x.strip() for x in srf_file_path.splitlines()))
    stoch_file = filter(None, (x.strip() for x in stoch_file_path.splitlines()))

    #    srf_stoch_pairs=[(srf_file_path,stoch_file_path)]
    # TODO: a quick workaround to make sure it takes only one srf
    # TODO: fix these logic when new config params versioning is properly impelmented

    print srf_file, stoch_file

    params_vel_path = os.path.join(vel_mod_dir, params_vel)

    execfile(params_vel_path, globals())

    sim_dir = os.path.join(user_root, run_name)

    yes_statcords = True
    yes_model_params = False

    vel_mod_params_dir = vel_mod_dir

    srf_dir = srf_default_dir  # the above is perhaps unnecessary
    create_params_dict(args.version, sim_dir, event_name, run_name, run_dir, vel_mod_dir, srf_dir, srf_file, stoch_file,
                       params_vel_path, stat_file_path, vs30_file_path, vs30ref_file_path, MODEL_LAT, MODEL_LON,
                       MODEL_ROT, hh, nx,
                       ny, nz, sufx, sim_duration, flo, vel_mod_params_dir, yes_statcords, yes_model_params)
    print "Installation completed"
    show_instruction(sim_dir)


if __name__ == '__main__':

    parser = argparse.ArgumentParser()

    parser.add_argument('--user_root', type=str, default=None, help="the path to where to install the simulation")
    parser.add_argument('--sim_cfg', type=str, default=None,
                        help="abs path to a file that contains all the config needed for a single sim install")

    parser.add_argument('--srf_dir', type=str, default=None, help="path that contains folders of faults/*.srf")
    parser.add_argument('--vm_dir', type=str, default=None, help="path that contains VMs, params_vel must be present")
    parser.add_argument('--v1d_dir', type=str, default=None)
    parser.add_argument('--station_dir', type=str, default=None)
    parser.add_argument('--version', type=str, default=None, help="version of simulation. eg.'gmsim_v18.5.3'")

    args = parser.parse_args()

    # If the additional options provided, check if the folder exist
    for arg in vars(args):
        if arg != 'version':
            path_to_check = getattr(args, arg)
            if path_to_check is not None:
                if not os.path.exists(path_to_check):
                    print "Error: path not exsist: %s" % path_to_check
                    sys.exit()
                else:
                    print "%s is set to %s" % (arg, path_to_check)
            else:
                continue

    # change corresponding variables to the args provided

    if args.user_root != None:
        # TODO:bad hack, fix this with parsing
        user_root = args.user_root

    if args.srf_dir != None:
        # TODO:bad hack, fix this with parsing
        srf_default_dir = args.srf_dir

    if args.vm_dir != None:
        # TODO:bad hack, fix this with parsing
        vel_mod_dir = args.vm_dir

    if args.v1d_dir != None:
        # TODO:bad hack, fix this with parsing
        v_mod_1d_dir = args.v1d_dir

    if args.station_dir != None:
        # TODO:bad hack, fix this with parsing
        stat_dir = args.station_dir
    print 'test'
    # if sim_cfg parsed, run main_remote(which has no selection)
    if args.sim_cfg != None:
        cfg = args.sim_cfg
        # check if the cfg exist, to prevent break
        if not os.path.exists(cfg):
            print "Error: No such file exists: %s" % cfg
            sys.exit()
        else:
            main_remote(cfg)
    else:
        main_local()
