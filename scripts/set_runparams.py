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
from collections import OrderedDict

sys.path.append(os.path.abspath(os.path.curdir))
from shutil import copyfile
from shared_workflow import shared
from qcore import utils

params_uncertain = 'params_uncertain.py'
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
    #import params_base
    sys.path.append(sim_dir)
   # params_base = __import__('params_base', globals(), locals(), [], -1)

    # attempt to append template file before importing params
   # try:
        # throws NameError if var not set, AssertionError if blank
    #    assert (params_override != '')
        # copy to temp file
     #   copyfile('params.py', 'params_joined.py')
        # append to temp
      #  with open('params_joined.py', 'a') as fp:
       #     with open('params_override_' + params_override + '.py', 'r') as tp:
        #        fp.write(tp.readlines())
        # import temp
     #   import params_joined
      #  os.remove('params_joined.py')
   # except (AssertionError, NameError, ImportError, OSError):
        #import params
    #    params = __import__('params', globals(), locals(), [], -1)

    p1 = {}
    p2 = {}
    fault_params_dict = utils.load_yaml('sim_params.yaml')
    for i, srf_file in enumerate(fault_params_dict['srf_file']):
        #skip all logic if a specific srf_name is provided
        if srf_name != None and srf_name != os.path.splitext(basename(srf_file))[0]:
            continue
        srf_file_basename = os.path.splitext(os.path.basename(srf_file))[0]  # take the filename only
        fault_params_dict['lf']= OrderedDict()
        fault_params_dict['lf']['lf_sim_dir'] = fault_params_dict['lf_sim_root_dir']
        #shared.verify_user_dirs(fault_params_dict['lf']['lf_sim_dir'])
        fault_params_dict['lf']['restart_dir'] = os.path.join(fault_params_dict['lf']['lf_sim_dir'], 'Restart')
        fault_params_dict['lf']['bin_output'] = os.path.join(fault_params_dict['lf']['lf_sim_dir'], 'OutBin')
        fault_params_dict['lf']['SEISDIR'] = fault_params_dict['lf']['bin_output']
        fault_params_dict['lf']['ts_file'] = os.path.join(fault_params_dict['lf']['bin_output'], fault_params_dict['run_name'] + '_xyts.e3d')  # the file created by merge_ts
        fault_params_dict['lf']['log_dir'] = os.path.join(fault_params_dict['lf']['lf_sim_dir'], 'Rlog')
        fault_params_dict['lf']['slipout_dir'] = os.path.join(fault_params_dict['lf']['lf_sim_dir'], 'SlipOut')
        fault_params_dict['lf']['vel_dir'] = os.path.join(fault_params_dict['lf']['lf_sim_dir'], 'Vel')
        fault_params_dict['lf']['t_slice_dir'] = os.path.join(fault_params_dict['lf']['lf_sim_dir'], 'TSlice')
        fault_params_dict['lf']['plot_ps_dir'] = os.path.join(fault_params_dict['lf']['t_slice_dir'], 'PlotFiles')
        fault_params_dict['lf']['plot_png_dir'] = os.path.join(fault_params_dict['lf']['t_slice_dir'], 'Png')
        fault_params_dict['lf']['ts_out_dir'] = os.path.join(fault_params_dict['lf']['t_slice_dir'], 'TSFiles')
        fault_params_dict['lf']['ts_out_prefix'] = os.path.join(fault_params_dict['lf']['ts_out_dir'], fault_params_dict['run_name'])
        fault_params_dict['lf']['FILELIST'] = os.path.join(fault_params_dict['lf']['lf_sim_dir'], 'fdb.filelist')
        fault_params_dict['lf']['lf_vel_resume'] = True

        fault_params_dict['emod3d']['version'] = fault_params_dict['version'] + '-mpi'

        fault_params_dict['emod3d']['name'] = fault_params_dict['run_name']
        fault_params_dict['emod3d']['n_proc'] = 512

        fault_params_dict['emod3d']['nx'] = fault_params_dict['vm']['nx']
        fault_params_dict['emod3d']['ny'] = fault_params_dict['vm']['ny']
        fault_params_dict['emod3d']['nz'] = fault_params_dict['vm']['nz']
        fault_params_dict['emod3d']['h'] = fault_params_dict['vm']['hh']
        fault_params_dict['emod3d']['dt'] = 0.0200
        fault_params_dict['emod3d']['nt'] = str(int(round(float(fault_params_dict['sim_duration'])/float(fault_params_dict['emod3d']['dt']))))
        fault_params_dict['emod3d']['bfilt'] = 4
        fault_params_dict['emod3d']['flo'] = fault_params_dict['flo']
        fault_params_dict['emod3d']['fhi'] = 0.0

        fault_params_dict['emod3d']['bforce'] = 0
        fault_params_dict['emod3d']['pointmt'] = 0
        fault_params_dict['emod3d']['dblcpl'] = 0
        fault_params_dict['emod3d']['ffault'] = 2
        fault_params_dict['emod3d']['faultfile'] = srf_file

        fault_params_dict['emod3d']['model_style'] = 1
        # only for the 1D velocity model
        # 'model=' + FD_VMODFILE, \
        fault_params_dict['emod3d']['vmoddir'] = fault_params_dict['vel_mod_dir']
        fault_params_dict['emod3d']['pmodfile'] = 'vp3dfile.p'
        fault_params_dict['emod3d']['smodfile'] = 'vs3dfile.s'
        fault_params_dict['emod3d']['dmodfile'] = 'rho3dfile.d'
        fault_params_dict['emod3d']['qpfrac'] = 100
        fault_params_dict['emod3d']['qsfrac'] = 50
        fault_params_dict['emod3d']['qpqs_factor'] = 2.0
        fault_params_dict['emod3d']['fmax'] = 25.0
        fault_params_dict['emod3d']['fmin'] = 0.01
        fault_params_dict['emod3d']['vmodel_swapb'] = 0

        fault_params_dict['emod3d']['modellon'] = fault_params_dict['vm']['MODEL_LON']
        fault_params_dict['emod3d']['modellat'] = fault_params_dict['vm']['MODEL_LAT']
        fault_params_dict['emod3d']['modelrot'] = fault_params_dict['vm']['MODEL_ROT']

        fault_params_dict['emod3d']['enable_output_dump'] = 1
        fault_params_dict['emod3d']['dump_itinc'] = 4000
        fault_params_dict['emod3d']['main_dump_dir'] = fault_params_dict['lf']['bin_output']
        fault_params_dict['emod3d']['nseis'] = 1
        fault_params_dict['emod3d']['seiscords'] = fault_params_dict['stat_coords']
        fault_params_dict['emod3d']['user_scratch'] = os.path.join(fault_params_dict['user_root'], 'scratch')
        fault_params_dict['emod3d']['seisdir'] = os.path.join(fault_params_dict['emod3d']['user_scratch'], fault_params_dict['run_name'], srf_file_basename, 'SeismoBin')
        fault_params_dict['emod3d']['ts_xy'] = 1
        fault_params_dict['emod3d']['iz_ts'] = 1
        fault_params_dict['emod3d']['ts_xz'] = 0
        fault_params_dict['emod3d']['iy_ts'] = 2
        fault_params_dict['emod3d']['ts_yz'] = 0
        fault_params_dict['emod3d']['ix_ts'] = 99
        fault_params_dict['emod3d']['dtts'] = 20
        fault_params_dict['emod3d']['dxts'] = 5
        fault_params_dict['emod3d']['dyts'] = 5
        fault_params_dict['emod3d']['dzts'] = 1
        fault_params_dict['emod3d']['ts_start'] = 0
        fault_params_dict['emod3d']['ts_inc'] = 1
        fault_params_dict['emod3d']['ts_total'] = str(int(float(fault_params_dict['sim_duration']) / (float(fault_params_dict['emod3d']['dt']) * float(fault_params_dict['emod3d']['dt_ts']))))
        fault_params_dict['emod3d']['ts_file'] = fault_params_dict['lf']['ts_file']
        fault_params_dict['emod3d']['ts_out_dir'] = fault_params_dict['lf']['ts_out_dir']
        fault_params_dict['emod3d']['ts_out_prefix'] = fault_params_dict['lf']['ts_out_prefix']
        fault_params_dict['emod3d']['swap_bytes'] = 0
        fault_params_dict['emod3d']['lonlat_out'] = 1
        fault_params_dict['emod3d']['scale'] = 1
        fault_params_dict['emod3d']['enable_restart'] = 1
        fault_params_dict['emod3d']['restartdir'] = fault_params_dict['lf']['restart_dir']
        fault_params_dict['emod3d']['restart_itinc'] = 20000
        fault_params_dict['emod3d']['read_restart'] = 0
        fault_params_dict['emod3d']['restartname'] = fault_params_dict['run_name']
        fault_params_dict['emod3d']['logdir'] = fault_params_dict['lf']['log_dir']
        fault_params_dict['emod3d']['slipout'] = fault_params_dict['lf']['slipout_dir'] + '/slipout-k2'
        # extras found in default parfile
        fault_params_dict['emod3d']['span'] = 1
        fault_params_dict['emod3d']['intmem'] = 1
        fault_params_dict['emod3d']['maxmem'] = 1500
        fault_params_dict['emod3d']['order'] = 4
        fault_params_dict['emod3d']['model_style'] = 1
        fault_params_dict['emod3d']['elas_only'] = 0
        fault_params_dict['emod3d']['freesurf'] = 1
        fault_params_dict['emod3d']['dampwidth'] = 0
        fault_params_dict['emod3d']['qbndmax'] = 100.0
        fault_params_dict['emod3d']['stype'] = '2tri-p10-h20'
        fault_params_dict['emod3d']['tzero'] = 0.6
        fault_params_dict['emod3d']['geoproj'] = 1
        fault_params_dict['emod3d']['report'] = 100
        fault_params_dict['emod3d']['all_in_one'] = 1
        fault_params_dict['emod3d']['xseis'] = 0
        fault_params_dict['emod3d']['iy_xs'] = 60
        fault_params_dict['emod3d']['iz_xs'] = 1
        fault_params_dict['emod3d']['dxout'] = 1
        fault_params_dict['emod3d']['yseis'] = 0
        fault_params_dict['emod3d']['ix_ys'] = 100
        fault_params_dict['emod3d']['iz_ys'] = 1
        fault_params_dict['emod3d']['dyout'] = 1
        fault_params_dict['emod3d']['zseis'] = 0
        fault_params_dict['emod3d']['ix_zs'] = 100
        fault_params_dict['emod3d']['iy_zs'] = 50
        fault_params_dict['emod3d']['dzout'] = 1
        fault_params_dict['emod3d']['ts_xz'] = 0
        fault_params_dict['emod3d']['iy_ts'] = 2
        fault_params_dict['emod3d']['ts_yz'] = 0
        fault_params_dict['emod3d']['ix_ts'] = 99
        # other locations
        fault_params_dict['emod3d']['wcc_prog_dir'] = os.path.join(fault_params_dict['global_root'], fault_params_dict['tools_dir'])
        fault_params_dict['emod3d']['vel_mod_params_dir'] = fault_params_dict['vm']['vel_mod_params_dir']
        fault_params_dict['emod3d']['global_root'] = fault_params_dict['global_root']
        fault_params_dict['emod3d']['sim_dir'] = fault_params_dict['sim_dir']
        # fault_params_dict['emod3d']['fault_file']= fault_file
        fault_params_dict['emod3d']['stat_file'] = fault_params_dict['stat_file']
        fault_params_dict['emod3d']['grid_file'] = fault_params_dict['vm']['GRIDFILE']
        fault_params_dict['emod3d']['model_params'] = fault_params_dict['vm']['MODEL_PARAMS']
        fault_params_dict['srf_file'] = srf_file
        shared.write_to_py(os.path.join(fault_params_dict['lf_sim_root_dir'], 'e3d.par'), fault_params_dict['emod3d'])
        # pop duplicated params before writing to yaml file
        fault_params_dict['emod3d'].pop('faultfile')
        fault_params_dict['emod3d'].pop('global_root')
        fault_params_dict['emoded'].pop('logdir')
        fault_params_dict['emoded'].pop('main_dump_dir')
        fault_params_dict['emoded'].pop('model_params')
        fault_params_dict['emoded'].pop('seiscords')
        fault_params_dict['emoded'].pop('vel_mod_params_dir')
        fault_params_dict['emoded'].pop('vmoddir')
        fault_params_dict['emoded'].pop('wcc_prog_dir')         
        utils.dump_yaml(fault_params_dict, os.path.join(fault_params_dict['sim_dir'], 'sim_params.yaml')) 


if __name__ == '__main__':
    sim_dir = os.getcwd()
  #  create_run_parameters(sim_dir)
    extend_yaml(sim_dir)

