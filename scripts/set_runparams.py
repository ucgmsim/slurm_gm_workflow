#!/usr/bin/env python2
"""
Generates 'e3d.par' from the default set, appending new key value pairs of parameters.
@author Viktor Polak, Sung Bae
@date 6 April 2016
Replaces set_runparams.csh. Converted to python, handles params set in separate file.
USAGE: edit params.py (not this file), execute this file from the same directory.
ISSUES: remove default values in e3d_default.par where not needed.
"""

from __future__ import print_function
import sys
import os.path
from os.path import basename

sys.path.append(os.path.abspath(os.path.curdir))
from shutil import copyfile
from shared_workflow import shared, write_template
from qcore import utils

from shared_workflow import load_config
workflow_config = load_config.load(os.path.dirname(os.path.realpath(__file__)), "workflow_config.json")
global_root = workflow_config["global_root"]
tools_dir = os.path.join(global_root, 'opt/maui/emod3d/3.0.4-gcc/bin')
emod3d_version = workflow_config["emod3d_version"]

#params_uncertain = 'params_uncertain.py'
#try:
#    from params_base import *
#except ImportError:
#    print(sys.path)
#    exit(1)


def create_run_parameters(sim_dir, srf_name=None):
    #import params_base
    sys.path.append(sim_dir)
    params_base = __import__('params_base', globals(), locals(), [], -1)

    # attempt to append template file before importing params
    try:
        # throws NameError if var not set, AssertionError if blank
        assert (params_override != '')
        # copy to temp file
        copyfile('params.py', 'params_joined.py')
        # append to temp
        with open('params_joined.py', 'a') as fp:
            with open('params_override_' + params_override + '.py', 'r') as tp:
                fp.write(tp.readlines())
        # import temp
        import params_joined
        os.remove('params_joined.py')
    except (AssertionError, NameError, ImportError, OSError):
        #import params 
        params = __import__('params', globals(), locals(), [], -1)

    p1 = {}
    p2 = {}
    for i, srf_file in enumerate(params.srf_files):
        #skip all logic if a specific srf_name is provided
        if srf_name != None and srf_name != os.path.splitext(basename(srf_file))[0]:
            continue
        srf_file_basename = os.path.splitext(os.path.basename(srf_file))[0]  # take the filename only
        #TODO: make it read lf_sim_dir from the new params version 
        p1['lf_sim_dir'] = params_base.lf_sim_root_dir
        shared.verify_user_dirs([p1['lf_sim_dir']])

        p1['restart_dir'] = os.path.join(p1['lf_sim_dir'], 'Restart')
        p1['bin_output'] = os.path.join(p1['lf_sim_dir'], 'OutBin')
        p1['SEISDIR'] = p1['bin_output']
        p1['ts_file'] = os.path.join(p1['bin_output'], params_base.run_name + '_xyts.e3d')  # the file created by merge_ts

        p1['log_dir'] = os.path.join(p1['lf_sim_dir'], 'Rlog')
        p1['slipout_dir'] = os.path.join(p1['lf_sim_dir'], 'SlipOut')
        p1['vel_dir'] = os.path.join(p1['lf_sim_dir'], 'Vel')
        p1['t_slice_dir'] = os.path.join(p1['lf_sim_dir'], 'TSlice')
        # output dirs and resolution (dpi)
        p1['plot_ps_dir'] = os.path.join(p1['t_slice_dir'], 'PlotFiles')  # only written to e3d.par
        p1['plot_png_dir'] = os.path.join(p1['t_slice_dir'], 'Png')  # only written to e3d.par

        p1['ts_out_dir'] = os.path.join(p1['t_slice_dir'], 'TSFiles')
        p1['ts_out_prefix'] = os.path.join(p1['ts_out_dir'], params_base.run_name)
        p1['FILELIST'] = os.path.join(p1['lf_sim_dir'], 'fdb.filelist')
        p1['lf_vel_resume'] = True

        shared.write_to_py(os.path.join(p1['lf_sim_dir'], params_uncertain), p1)

        p2['version'] = params_base.version + '-mpi'
        p2['name'] = params_base.run_name
        p2['nproc'] = params.n_proc

        p2['nx'] = params.nx
        p2['ny'] = params.ny
        p2['nz'] = params.nz
        p2['h'] = params.hh
        p2['nt'] = params.nt
        p2['dt'] = params.dt
        p2['bfilt'] = 4
        p2['flo'] = params.flo
        p2['fhi'] = 0.0

        p2['bforce'] = 0
        p2['pointmt'] = 0
        p2['dblcpl'] = 0
        p2['ffault'] = 2
        p2['faultfile'] = srf_file

        p2['model_style'] = 1
        # only for the 1D velocity model
        # 'model=' + FD_VMODFILE, \
        p2['vmoddir'] = params_base.vel_mod_dir
        p2['pmodfile'] = params.PMOD
        p2['smodfile'] = params.SMOD
        p2['dmodfile'] = params.DMOD
        p2['qpfrac'] = 100
        p2['qsfrac'] = 50
        p2['qpqs_factor'] = 2.0
        p2['fmax'] = 25.0
        p2['fmin'] = 0.01
        p2['vmodel_swapb'] = 0

        p2['modellon'] = params_base.MODEL_LON
        p2['modellat'] = params_base.MODEL_LAT
        p2['modelrot'] = params_base.MODEL_ROT

        p2['enable_output_dump'] = 1
        p2['dump_itinc'] = params.DUMP_ITINC
        p2['main_dump_dir'] = p1['bin_output']
        p2['nseis'] = 1
        p2['seiscords'] = params_base.stat_coords
        p2['seisdir'] = os.path.join(params.user_scratch, params_base.run_name, srf_file_basename, 'SeismoBin')
        p2['ts_xy'] = params.ts_xy
        p2['iz_ts'] = params.iz_ts
        p2['ts_xz'] = params.ts_xz
        p2['iy_ts'] = params.iy_ts
        p2['ts_yz'] = params.ts_yz
        p2['ix_ts'] = params.ix_ts
        p2['dtts'] = params.dt_ts
        p2['dxts'] = params.dx_ts
        p2['dyts'] = params.dy_ts
        p2['dzts'] = params.dz_ts
        p2['ts_start'] = params.ts_start
        p2['ts_inc'] = params.ts_inc
        p2['ts_total'] = params.ts_total
        p2['ts_file'] = p1['ts_file']
        p2['ts_out_dir'] = p1['ts_out_dir']
        p2['ts_out_prefix'] = p1['ts_out_prefix']
        p2['swap_bytes'] = params.swap_bytes
        p2['lonlat_out'] = params.lonlat_out
        p2['scale'] = params.scale
        p2['enable_restart'] = params.ENABLE_RESTART
        p2['restartdir'] = p1['restart_dir']
        p2['restart_itinc'] = params.RESTART_ITINC
        p2['read_restart'] = params.READ_RESTART
        p2['restartname'] = params_base.run_name
        p2['logdir'] = p1['log_dir']
        p2['slipout'] = p1['slipout_dir'] + '/slipout-k2'
        # extras found in default parfile
        p2['span'] = 1
        p2['intmem'] = 1
        p2['maxmem'] = 1500
        p2['order'] = 4
        p2['model_style'] = 1
        p2['elas_only'] = 0
        p2['freesurf'] = 1
        p2['dampwidth'] = 0
        p2['qbndmax'] = 100.0
        p2['stype'] = '2tri-p10-h20'
        p2['tzero'] = 0.6
        p2['geoproj'] = 1
        p2['report'] = 100
        p2['all_in_one'] = 1
        p2['xseis'] = 0
        p2['iy_xs'] = 60
        p2['iz_xs'] = 1
        p2['dxout'] = 1
        p2['yseis'] = 0
        p2['ix_ys'] = 100
        p2['iz_ys'] = 1
        p2['dyout'] = 1
        p2['zseis'] = 0
        p2['ix_zs'] = 100
        p2['iy_zs'] = 50
        p2['dzout'] = 1
        p2['ts_xz'] = 0
        p2['iy_ts'] = 2
        p2['ts_yz'] = 0
        p2['ix_ts'] = 99
        # other locations
        p2['wcc_prog_dir'] = params.wcc_prog_dir
        p2['vel_mod_params_dir'] = params_base.vel_mod_params_dir
        p2['global_root'] = params_base.global_root
        p2['sim_dir'] = params_base.sim_dir
        # p2['fault_file']= fault_file
        p2['stat_file'] = params_base.stat_file
        p2['grid_file'] = params_base.GRIDFILE
        p2['model_params'] = params_base.MODELPARAMS

        shared.write_to_py(os.path.join(p1['lf_sim_dir'], params.parfile), p2)


def extend_yaml(sim_dir, srf_name=None):
    sys.path.append(sim_dir)
    params = utils.load_params('root_params.yaml', 'fault_params.yaml', 'sim_params.yaml')
    utils.update(params, utils.load_params(os.path.join(params.vel_mod_dir,'vm_params.yaml')))
    srf_file = params.srf_file[0]
    e3d_yaml = os.path.join(workflow_config['templates_dir'], 'emod3d_defaults_{}.yaml'.format(params.version))
    e3d_dict = utils.load_yaml(e3d_yaml)

    #skip all logic if a specific srf_name is provided
    if srf_name is None or srf_name == os.path.splitext(basename(srf_file))[0]:

        e3d_dict['version'] = emod3d_version + '-mpi'

        e3d_dict['name'] = params.run_name
        e3d_dict['n_proc'] = 512

        e3d_dict['nx'] = params.nx
        e3d_dict['ny'] = params.ny
        e3d_dict['nz'] = params.nz
        e3d_dict['h'] = params.hh
        e3d_dict['dt'] = params.dt
        e3d_dict['nt'] = str(int(round(float(params.sim_duration)/float(params.dt))))
        e3d_dict['bfilt'] = 4

        e3d_dict['faultfile'] = srf_file

        e3d_dict['vmoddir'] = params.vel_mod_dir

        e3d_dict['modellon'] = params.MODEL_LON
        e3d_dict['modellat'] = params.MODEL_LAT
        e3d_dict['modelrot'] = params.MODEL_ROT


        e3d_dict['main_dump_dir'] = os.path.join(params.sim_dir, 'LF', 'OutBin')
        e3d_dict['seiscords'] = params.stat_coords
        e3d_dict['user_scratch'] = os.path.join(params.user_root, 'scratch')
        e3d_dict['seisdir'] = os.path.join(e3d_dict['user_scratch'], params.run_name, srf_file_basename, 'SeismoBin')

        e3d_dict['ts_total'] = str(int(float(params.sim_duration) / (float(e3d_dict['dt']) * float(e3d_dict['dtts']))))
        e3d_dict['ts_file'] = os.path.join(e3d_dict['main_dump_dir'], params.run_name + '_xyts.e3d')
        e3d_dict['ts_out_dir'] = os.path.join(params.sim_dir, 'LF', 'TSlice', 'TSFiles')

        e3d_dict['restartdir'] = os.path.join(params.sim_dir, 'LF', 'Restart')

        e3d_dict['restartname'] = params.run_name
        e3d_dict['logdir'] = os.path.join(params.sim_dir, 'LF', 'Rlog')
        e3d_dict['slipout'] = os.path.join(params.sim_dir, 'LF', 'SlipOut', 'slipout-k2')

        # other locations
        e3d_dict['wcc_prog_dir'] = tools_dir
        e3d_dict['vel_mod_params_dir'] = params.vel_mod_dir
        e3d_dict['global_root'] = global_root
        e3d_dict['sim_dir'] = params.sim_dir
        e3d_dict['stat_file'] = params.stat_file
        e3d_dict['grid_file'] = params.GRIDFILE
        e3d_dict['model_params'] = params.MODEL_PARAMS
        shared.write_to_py(os.path.join(params.sim_dir, 'LF', 'e3d.par'), e3d_dict)


if __name__ == '__main__':
    sim_dir = os.getcwd()
  # create_run_parameters(sim_dir)
    extend_yaml(sim_dir)

