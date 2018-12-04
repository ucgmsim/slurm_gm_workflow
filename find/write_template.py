import os
import yaml
import sys
from qcore import utils


def get_e3d_defaults():
    d = {}
    d['all_in_one'] = 1
    d['bflit'] = 4
    d['bforce'] = 0
    d['dampwidth'] = 0
    d['dblcpl'] = 0
    d['dmodfile'] = 'rho3dfile.d'
    d['pmodfile'] = 'vp3dfile.p'
    d['smodfile'] = 'vs3dfile.s'
    d['dtts'] = 20
    d['dxts'] = 5
    d['dyts'] = 5
    d['dzts'] = 1
    d['dump_itinc'] = 4000
    d['dxout'] = 1
    d['dyout'] = 1
    d['dzout'] = 1
    d['elas_only'] = 0
    d['enable_output_dump'] = 1
    d['enable_restart'] = 1
    d['ffault'] = 2
    d['fhi'] = 0.0
    d['flo'] = 0.25  #to remove. should be retieved from srfile
    d['fmax'] = 25
    d['fmin'] = 0.01
    d['freesurf'] = 1
    d['geoproj'] = 1
    d['global_root'] = '/nesi/project/nesi00213'
    d['intmem'] = 1
    d['ix_ts'] = 99
    d['ix_ys'] = 100
    d['ix_zs'] = 100
    d['iy_ts'] = 2
    d['iy_xs'] = 60
    d['iy_zs'] = 50
    d['iz_ts'] = 1
    d['iz_xs'] = 1
    d['iz_ys'] = 1
    d['lonlat_out'] = 1
    d['maxmem'] = 1500
    d['model_style'] = 1
    d['n_proc'] = 512  #test to remove from e3d.par
    d['nseis'] = 1
    d['order'] = 4
    d['pointmt'] = 0
    d['qbndmax'] = 100.0
    d['qpfrac'] = 100
    d['qpqs_factor'] = 2.0
    d['qsfrac'] = 50
    d['read_restart'] = 0
    d['report'] = 100
    d['restart_itinc'] = 20000
    d['scale'] = 1
    d['span'] = 1
    d['stype'] = '2tri-p10-h20'
    d['swap_bytes'] = 0
    d['ts_inc'] = 1
    d['ts_start'] = 0
    d['ts_total'] = 218
    d['ts_xy'] = 1
    d['ts_xz'] = 0
    d['ts_yz'] = 0
    d['tzero'] = 0.6
    d['vmodel_swapb'] = 0
    d['xseis'] = 0
    d['yseis'] = 0
    d['zseis'] = 0
    return d


# d = get_e3d_defatuls()
# utils.dump_yaml(d, '/home/melody.zhu/slurm_gm_workflow/shared_workflow/emod3d_defaults.yaml')
