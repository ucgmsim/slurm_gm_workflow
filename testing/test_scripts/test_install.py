# TODO: to decide what to do with user input prompts

import inspect
import os
import pickle

from scripts import install
from testing.test_common_set_up import INPUT, OUTPUT, set_up, get_input_params, get_bench_output


def test_q_select_rupmodel_dir(set_up):
    func_name = 'q_select_rupmodel_dir'
    for root_path, _ in set_up:
        with open(os.path.join(root_path, INPUT, func_name + "_rupmodel_dir.P"), "rb") as load_file:
            input_param = pickle.load(load_file)
            test_output = install.q_select_rupmodel_dir(input_param)
        bench_output = get_bench_output(root_path, func_name)
        assert test_output == bench_output


def test_q_select_rupmodel(set_up):
    func_name = 'q_select_rupmodel'
    params = inspect.getfullargspec(install.q_select_rupmodel).args
    for root_path, _ in set_up:
        input_params = get_input_params(root_path, func_name, params)
        test_output = install.q_select_rupmodel(*input_params)
        bench_output = get_bench_output(root_path, func_name)
        assert test_output == bench_output


def test_q_select_vel_model(set_up):
    func_name = 'q_select_vel_model'
    for root_path, _ in set_up:
        with open(os.path.join(root_path, INPUT, func_name + "_vel_mod_dir.P"), "rb") as load_file:
            input_param = pickle.load(load_file)
            test_output = install.q_select_vel_model(input_param)
        bench_output = get_bench_output(root_path, func_name)
        assert test_output == bench_output


def test_q_select_stat_file(set_up):
    func_name = 'q_select_stat_file'
    params = inspect.getfullargspec(install.q_select_stat_file).args
    for root_path, _ in set_up:
        input_params = get_input_params(root_path, func_name, params)
        test_output = install.q_select_stat_file(*input_params)
        bench_output = get_bench_output(root_path, func_name)
        assert test_output == bench_output


def test_q_select_vs30_file(set_up):
    func_name = 'q_select_vs30_file'
    params = inspect.getfullargspec(install.q_select_vs30_file).args
    for root_path, _ in set_up:
        input_params = get_input_params(root_path, func_name, params)
        test_output = install.q_select_vs30_file(*input_params)
        bench_output = get_bench_output(root_path, func_name)
        assert test_output == bench_output


def test_q_get_run_name(set_up):
    func_name = 'q_get_run_name'
    params = inspect.getfullargspec(install.q_get_run_name).args
    for root_path, _ in set_up:
        input_params = get_input_params(root_path, func_name, params)
        test_output = install.q_get_run_name(*input_params)
        bench_output = get_bench_output(root_path, func_name)
        assert test_output == bench_output

