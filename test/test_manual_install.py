import os
import numpy as np
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
        out, err = shared.exe('diff {} {}'.format(os.path.join(bench_path,bench_e3ds[i]), os.path.join(test_path,test_e3ds[i])))
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


test_lf_bin('/home/melody.zhu/Albury_newman/Albury_VM_home_melodypzhu_Albury_Data_VMs_Albury-h0p4_EMODv3p0p4_181206/LF/OutBin', '/home/melody.zhu/Albury_bench/Runs/Albury/LF/Albury_HYP15-21_S1384/OutBin')

test_e3ds('/home/melody.zhu/Albury_newman/Albury_VM_home_melodypzhu_Albury_Data_VMs_Albury-h0p4_EMODv3p0p4_181206/LF/OutBin', '/home/melody.zhu/Albury_bench/Runs/Albury/LF/Albury_HYP15-21_S1384/OutBin')

test_bb_bin('/home/melody.zhu/Albury_newman/Albury_VM_home_melodypzhu_Albury_Data_VMs_Albury-h0p4_EMODv3p0p4_181206/BB/Acc/BB.bin', '/home/melody.zhu/Albury_bench/Runs/Albury/BB/Cant1D_d2-midQ_leer_hfnp2mm+_rvf0p8_sd50_k0p045/Albury_HYP15-21_S1384/Acc/BB.bin')

test_hf_bin('/home/melody.zhu/Albury_newman/Albury_VM_home_melodypzhu_Albury_Data_VMs_Albury-h0p4_EMODv3p0p4_181206/HF/Acc/HF.bin', '/home/melody.zhu/Albury_bench/Runs/Albury/HF/Cant1D_d2-midQ_leer_hfnp2mm+_rvf0p8_sd50_k0p045/Albury_HYP15-21_S1384/Acc/HF.bin')