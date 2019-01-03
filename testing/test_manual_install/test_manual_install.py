"""
python test_manual_install.py --bench_e3d /home/melody.zhu/Albury_bench/Runs/Albury/LF/Albury_HYP15-21_S1384/e3d.par --test_e3d /home/melody.zhu/Albury_newman/Albury_VM_home_melodypzhu_Albury_newman_Data_VMs_Albury-h0p4_EMODv3p0p4_181220/LF/e3d.par --bench_outbin /home/melody.zhu/Albury_bench/Runs/Albury/LF/Albury_HYP15-21_S1384/OutBin --test_outbin /home/melody.zhu/Albury_newman/Albury_VM_home_melodypzhu_Albury_newman_Data_VMs_Albury-h0p4_EMODv3p0p4_181220/LF/OutBin --bench_hf /home/melody.zhu/Albury_bench/Runs/Albury/HF/Cant1D_v2-midQ_leer_hfnp2mm+_rvf0p8_sd50_k0p045/Albury_HYP15-21_S1384/Acc/HF.bin --test_hf /home/melody.zhu/Albury_newman/Albury_VM_home_melodypzhu_Albury_newman_Data_VMs_Albury-h0p4_EMODv3p0p4_181220/HF/Acc/HF.bin --bench_bb /home/melody.zhu/Albury_bench/Runs/Albury/BB/Cant1D_v2-midQ_leer_hfnp2mm+_rvf0p8_sd50_k0p045/Albury_HYP15-21_S1384/Acc/BB.bin --test_bb /home/melody.zhu/Albury_newman/Albury_VM_home_melodypzhu_Albury_newman_Data_VMs_Albury-h0p4_EMODv3p0p4_181220/BB/Acc/BB.bin

"""

import os
import numpy as np
import argparse
import shutil
from qcore import timeseries
from qcore import shared
from qcore import utils


DIVIDER = '-' *20
TXT1 = 'txt1'
TXT2 = 'txt2'


def get_par_dict(e3d_par):
    d = {}
    with open(e3d_par, 'r') as e1:
        t1 = e1.readlines()
    for line in t1:
        k, v = line.strip().split("=")
        d[k] = v
    return d


def test_e3d_par(bench_e3d, test_e3d):
    print("{}testing e3d.par{}".format(DIVIDER, DIVIDER))
    d1 = get_par_dict(bench_e3d)
    d2 = get_par_dict(test_e3d)
    print("keys in bench but not in test are {}".format(set(d1.keys()).difference(set(d2.keys()))))
    print("keys in test but not in bench are {}".format(set(d2.keys()).difference(set(d1.keys()))))
    for k in d1.keys():
        try:
            if d2[k] != d1[k]:
                print("k bench, test", k, d1[k], d2[k])
        except KeyError:
            continue


def check_headers(s1, s2):
    """
    :param s1: timeseris obj
    :param s2: timeseris obj
    :return:
    """
    d1 = vars(s1)
    d2 = vars(s2)
    for k in d1.keys():
        try:
            if d1[k] != d2[k]:
                print("{}\nbench is {};\ntest is {}".format(k, d1[k], d2[k]))
        except ValueError:
            if not np.array_equal(d1[k], d2[k]):
                print("{}\nbench is {};\ntest is {}".format(k, d1[k], d2[k]))


def check_data(s1, s2):
    utils.setup_dir(TXT1)
    utils.setup_dir(TXT2)
    s1.all2txt(prefix='./{}/'.format(TXT1))
    s2.all2txt(prefix='./{}/'.format(TXT2))
    for f in os.listdir('txt1'):
        out, err = shared.exe('diff {} {}'.format(os.path.join('txt1', f), os.path.join('txt2', f)))
        
    shutil.rmtree(TXT1)
    shutil.rmtree(TXT2)


def test_lf_bin(bench_path, test_path):
    """
    :param bench_path: path to benchmark OutBin folder
    :param test_path: path to testing OutBin folder
    :return:
    """
    print("{}testing LF OutBin{}".format(DIVIDER, DIVIDER))
    l1 = timeseries.LFSeis(bench_path)
    l2 = timeseries.LFSeis(test_path)
    check_headers(l1, l2)
    check_data(l1, l2)


def test_e3ds(bench_path, test_path):
    """
    :param bench_path: path to benchmark OutBin folder
    :param test_path:
    :return:
    """
    print("{}testing e3d segments{}".format(DIVIDER, DIVIDER))
    bench_e3ds = sorted(os.listdir(bench_path))
    test_e3ds = sorted(os.listdir(test_path))
    assert len(bench_e3ds) == len(test_e3ds)
    logs = ''
    for i in range(len(bench_e3ds)):
        out, err = shared.exe(
            'diff {} {}'.format(os.path.join(bench_path, bench_e3ds[i]), os.path.join(test_path, test_e3ds[i])))
        logs += out + err

# assert logs == ''


def test_hf_bin(bench_path, test_path):
    """
    :param bench_path: path to benchmark BB.bin file
    :param test_path:
    :return:
    """
    print("{}testing HF bin{}".format(DIVIDER, DIVIDER))
    h1 = timeseries.HFSeis(bench_path)
    h2 = timeseries.HFSeis(test_path)
    check_headers(h1, h2)
   # check_data(h1, h2)


def test_bb_bin(bench_path, test_path):
    """
    :param bench_path: path to benchmark HF.bin file
    :param test_path:
    :return:
    """
    print("{}testing BB.bin{}".format(DIVIDER, DIVIDER))
    b1 = timeseries.BBSeis(bench_path)
    b2 = timeseries.BBSeis(test_path)
    check_headers(b1, b2)
    check_data(b1, b2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bench_e3d', help='path to benchmark e3d.par')
    parser.add_argument('--test_e3d', help='path to test e3d.par')
    parser.add_argument('--bench_outbin', help='path to benchmark OutBin')
    parser.add_argument('--test_outbin', help='path to test OutBin')
    parser.add_argument('--bench_hf', help='path to benchmark HF.bin')
    parser.add_argument('--test_hf', help='path to test HF.bin')
    parser.add_argument('--bench_bb', help='path to benchmark BB.bin')
    parser.add_argument('--test_bb', help='path to test BB.bin')

    args = parser.parse_args()

    if args.bench_e3d and args.test_e3d:
        assert os.path.isfile(args.bench_e3d)
        assert os.path.isfile(args.test_e3d)
        test_e3d_par(args.bench_e3d,args.test_e3d)

    if args.bench_outbin and args.test_outbin:
        assert os.path.isdir(args.bench_outbin)
        assert os.path.isdir(args.test_outbin)
        test_lf_bin(args.bench_outbin, args.test_outbin)
        test_e3ds(args.bench_outbin, args.test_outbin)

    if args.bench_hf and args.test_hf:
        assert os.path.isfile(args.bench_hf)
        assert os.path.isfile(args.test_hf)
        test_hf_bin(args.bench_hf, args.test_hf)

    if args.bench_bb and args.test_bb:
        assert os.path.isfile(args.bench_bb)
        assert os.path.isfile(args.test_bb)
        test_bb_bin(args.bench_bb, args.test_bb)


if __name__ == '__main__':
    main()

