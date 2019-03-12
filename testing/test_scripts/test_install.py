# TODO: to decide what to do with user input prompts

import inspect
import os
import pickle

from scripts import install
from testing.test_common_set_up import INPUT, OUTPUT, set_up, get_input_params, get_bench_output


def test_q_select_rupmodel_dir(set_up):
    func_name = 'q_select_rupmodel_dir'
    for root_path, realisation in set_up:
        with open(os.path.join(root_path, INPUT, func_name + "_rupmodel_dir.P"), "rb") as load_file:
            input_param = pickle.load(load_file)
            test_output = install.q_select_rupmodel_dir(input_param)
        bench_output = get_bench_output(root_path, func_name)
        assert test_output == bench_output