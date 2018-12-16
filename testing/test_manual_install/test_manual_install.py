"""
python test_manual_install.py --bench_outbin /home/melody.zhu/Albury_bench/Runs/Albury/LF/Albury_HYP15-21_S1384/OutBin --test_outbin /home/melody.zhu/Albury_newman/Albury_VM_home_melodypzhu_Albury_newman_Data_VMs_Albury-h0p4_EMODv3p0p4_181210/LF/OutBin --bench_hf /home/melody.zhu/Albury_bench/Runs/Albury/HF/Cant1D_v2-midQ_leer_hfnp2mm+_rvf0p8_sd50_k0p045/Albury_HYP15-21_S1384/Acc/HF.bin --test_hf /home/melody.zhu/Albury_newman/Albury_VM_home_melodypzhu_Albury_newman_Data_VMs_Albury-h0p4_EMODv3p0p4_181210/HF/Acc/HF.bin --bench_bb /home/melody.zhu/Albury_bench/Runs/Albury/BB/Cant1D_v2-midQ_leer_hfnp2mm+_rvf0p8_sd50_k0p045/Albury_HYP15-21_S1384/Acc/BB.bin --test_bb /home/melody.zhu/Albury_newman/Albury_VM_home_melodypzhu_Albury_newman_Data_VMs_Albury-h0p4_EMODv3p0p4_181210/BB/Acc/BB.bin

"""

import os
import numpy as np
import argparse
from qcore import timeseries
from qcore import shared


def check_values(s1, s2):
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


def test_lf_bin(bench_path, test_path):
    """
    :param bench_path: path to benchmark OutBin folder
    :param test_path: path to testing OutBin folder
    :return:
    """
    l1 = timeseries.LFSeis(bench_path)
    l2 = timeseries.LFSeis(test_path)
    check_values(l1, l2)


def test_e3ds(bench_path, test_path):
    """
    :param bench_path: path to benchmark OutBin folder
    :param test_path:
    :return:
    """
    bench_e3ds = sorted(os.listdir(bench_path))
    test_e3ds = sorted(os.listdir(test_path))
    assert len(bench_e3ds) == len(test_e3ds)
    logs = ''
    for i in range(len(bench_e3ds)):
        out, err = shared.exe(
            'diff {} {}'.format(os.path.join(bench_path, bench_e3ds[i]), os.path.join(test_path, test_e3ds[i])))
        logs += out + err
    assert logs == ''


def test_hf_bin(bench_path, test_path):
    """
    :param bench_path: path to benchmark BB.bin file
    :param test_path:
    :return:
    """
    h1 = timeseries.HFSeis(bench_path)
    h2 = timeseries.HFSeis(test_path)
    check_values(h1, h2)


def test_bb_bin(bench_path, test_path):
    """
    :param bench_path: path to benchmark HF.bin file
    :param test_path:
    :return:
    """
    b1 = timeseries.BBSeis(bench_path)
    b2 = timeseries.BBSeis(test_path)
    check_values(b1, b2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--bench_outbin', help='path to benchmark OutBin')
    parser.add_argument('--test_outbin', help='path to test OutBin')
    parser.add_argument('--bench_hf', help='path to benchmark HF.bin')
    parser.add_argument('--test_hf', help='path to test HF.bin')
    parser.add_argument('--bench_bb', help='path to benchmark BB.bin')
    parser.add_argument('--test_bb', help='path to test BB.bin')

    args = parser.parse_args()

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